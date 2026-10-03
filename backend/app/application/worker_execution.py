from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Callable

from app.application.execution import (
    ExecuteGenerationCommand,
    ExecuteGenerationJob,
    GenerationProviderResolver,
    LeaseOwnershipLost,
)
from app.application.reconciliation import (
    AmbiguousProviderOutcome,
    ProviderOperationSafety,
    ReconciliationQuery,
)
from app.application.persistence import ExecutionPersistenceTransaction
from app.application.ports import GenerationRequest, GenerationResult
from app.application.worker import GenerationJobQueue, QueueMessage, WorkerLeaseRepository
from app.domain.events import JobEvent, JobEventType
from app.domain.failure import Failure
from app.domain.generation import GenerationAttempt, GenerationStatus


class WorkerDeliveryStatus(str, Enum):
    ACKED = "ACKED"
    REQUEUED = "REQUEUED"
    NOT_CLAIMED = "NOT_CLAIMED"


@dataclass(frozen=True)
class WorkerDeliveryResult:
    status: WorkerDeliveryStatus
    message_id: str
    job_id: str
    reason: str


class ExecuteGenerationDelivery:
    """Own one queue delivery from lease claim through durable outcome/ACK.

    The queue is not authoritative. Every delivery re-reads durable job state.
    """

    def __init__(
        self,
        queue: GenerationJobQueue,
        leases: WorkerLeaseRepository,
        transaction_factory: Callable[[], ExecutionPersistenceTransaction],
        providers: GenerationProviderResolver,
        lease_duration: timedelta,
        provider_executor: Callable[[object, GenerationRequest, Callable[[datetime], bool]], GenerationResult] | None = None,
    ) -> None:
        if lease_duration <= timedelta(0):
            raise ValueError("lease_duration must be positive")
        self._queue = queue
        self._leases = leases
        self._transaction_factory = transaction_factory
        self._providers = providers
        self._lease_duration = lease_duration
        self._provider_executor = provider_executor

    def renew_lease(
        self,
        job_id: str,
        lease_token: str,
        now: datetime,
    ) -> bool:
        """Renew an active lease during long-running provider execution."""
        return self._leases.renew(
            job_id,
            lease_token,
            now,
            self._lease_duration,
        ) is not None

    def handle(
        self,
        message: QueueMessage,
        worker_id: str,
        now: datetime,
        inputs: dict[str, object],
        references: tuple[str, ...] = (),
        constraints: dict[str, object] | None = None,
    ) -> WorkerDeliveryResult:
        lease = self._leases.claim(
            message.job_id,
            worker_id,
            now,
            self._lease_duration,
        )
        if lease is None:
            return WorkerDeliveryResult(
                WorkerDeliveryStatus.NOT_CLAIMED,
                message.message_id,
                message.job_id,
                "active lease held by another worker",
            )

        tx = self._transaction_factory()
        try:
            job = tx.jobs.get(message.job_id)
            if job is None:
                self._queue.ack(message)
                return WorkerDeliveryResult(
                    WorkerDeliveryStatus.ACKED,
                    message.message_id,
                    message.job_id,
                    "job no longer exists in durable state",
                )

            if job.status in {GenerationStatus.CANCELLED, GenerationStatus.SUCCEEDED}:
                self._queue.ack(message)
                return WorkerDeliveryResult(
                    WorkerDeliveryStatus.ACKED,
                    message.message_id,
                    message.job_id,
                    f"job already terminal: {job.status.value}",
                )

            if job.status is GenerationStatus.RUNNING:
                job.recover_expired_lease()
                tx.append_event(
                    JobEvent(
                        event_id=f"{job.job_id}:recovered:{job.attempt_count}",
                        job_id=job.job_id,
                        event_type=JobEventType.RECOVERED,
                        occurred_at=now,
                        attempt_number=job.attempt_count,
                    )
                )
                tx.jobs.save(job)

            execution = ExecuteGenerationJob(
                tx,
                self._providers,
                provider_executor=(
                    None
                    if self._provider_executor is None
                    else lambda provider, request: self._provider_executor(
                        provider,
                        request,
                        lambda heartbeat_at: self.renew_lease(
                            message.job_id,
                            lease.lease_token,
                            heartbeat_at,
                        ),
                    )
                ),
            )
            result = execution.execute(
                ExecuteGenerationCommand(
                    job_id=message.job_id,
                    inputs=inputs,
                    references=references,
                    constraints=constraints,
                    lease_token=lease.lease_token,
                )
            )
            self._queue.ack(message)
            return WorkerDeliveryResult(
                WorkerDeliveryStatus.ACKED,
                message.message_id,
                message.job_id,
                "durable outcome committed before acknowledgement",
            )
        except LeaseOwnershipLost:
            tx.rollback()
            self._queue.release_or_requeue(message)
            return WorkerDeliveryResult(
                WorkerDeliveryStatus.REQUEUED,
                message.message_id,
                message.job_id,
                "lease ownership lost before terminal persistence",
            )
        except AmbiguousProviderOutcome as exc:
            tx.rollback()
            return self._handle_ambiguous_outcome(
                message=message,
                lease_token=lease.lease_token,
                now=now,
                outcome=exc,
            )
        except Exception:
            # ExecuteGenerationJob owns rollback for terminal persistence failures.
            # Other pre-execution failures have no durable mutation to recover.
            self._queue.release_or_requeue(message)
            return WorkerDeliveryResult(
                WorkerDeliveryStatus.REQUEUED,
                message.message_id,
                message.job_id,
                "execution or durable persistence failed",
            )
        finally:
            self._leases.release(message.job_id, lease.lease_token)


    def _handle_ambiguous_outcome(
        self,
        *,
        message: QueueMessage,
        lease_token: str,
        now: datetime,
        outcome: AmbiguousProviderOutcome,
    ) -> WorkerDeliveryResult:
        if outcome.contract.operation_safety is ProviderOperationSafety.IDEMPOTENT:
            self._queue.release_or_requeue(message)
            return WorkerDeliveryResult(
                WorkerDeliveryStatus.REQUEUED,
                message.message_id,
                message.job_id,
                "ambiguous idempotent provider outcome; stable operation key is reused",
            )

        if outcome.contract.operation_safety is ProviderOperationSafety.RECONCILABLE:
            if outcome.reconciliation is None:
                self._queue.release_or_requeue(message)
                return WorkerDeliveryResult(
                    WorkerDeliveryStatus.REQUEUED,
                    message.message_id,
                    message.job_id,
                    "reconciliation declared but no lookup port supplied",
                )
            try:
                lookup_tx = self._transaction_factory()
                job = lookup_tx.jobs.get(message.job_id)
                if job is None:
                    raise KeyError(f"generation job not found: {message.job_id}")
                key = job.idempotency_key
                lookup_tx.rollback()
                generation = outcome.reconciliation.reconcile(ReconciliationQuery(key))
            except Exception:
                self._queue.release_or_requeue(message)
                return WorkerDeliveryResult(
                    WorkerDeliveryStatus.REQUEUED,
                    message.message_id,
                    message.job_id,
                    "reconciliation lookup failed; no blind provider resubmission",
                )
            if generation is None:
                self._queue.release_or_requeue(message)
                return WorkerDeliveryResult(
                    WorkerDeliveryStatus.REQUEUED,
                    message.message_id,
                    message.job_id,
                    "no external operation found; redelivery may safely resubmit",
                )
            try:
                self._persist_reconciled_result(message, lease_token, now, generation)
            except Exception:
                self._queue.release_or_requeue(message)
                return WorkerDeliveryResult(
                    WorkerDeliveryStatus.REQUEUED,
                    message.message_id,
                    message.job_id,
                    "reconciled result could not be durably persisted",
                )
            self._queue.ack(message)
            return WorkerDeliveryResult(
                WorkerDeliveryStatus.ACKED,
                message.message_id,
                message.job_id,
                "existing external operation reconciled before acknowledgement",
            )

        try:
            self._persist_reconciliation_required(message, lease_token, now, outcome.provider)
        except Exception:
            self._queue.release_or_requeue(message)
            return WorkerDeliveryResult(
                WorkerDeliveryStatus.REQUEUED,
                message.message_id,
                message.job_id,
                "reconciliation-required outcome could not be durably persisted",
            )
        self._queue.ack(message)
        return WorkerDeliveryResult(
            WorkerDeliveryStatus.ACKED,
            message.message_id,
            message.job_id,
            "ambiguous non-reconcilable outcome recorded as reconciliation required",
        )

    def _persist_reconciled_result(
        self,
        message: QueueMessage,
        lease_token: str,
        now: datetime,
        generation: GenerationResult,
    ) -> None:
        tx = self._transaction_factory()
        try:
            tx.assert_lease_owner(message.job_id, lease_token)
            job = tx.jobs.get(message.job_id)
            if job is None:
                raise KeyError(f"generation job not found: {message.job_id}")
            job.start()
            started = GenerationAttempt.started(
                attempt_id=f"{job.job_id}:attempt-{job.attempt_count}",
                job_id=job.job_id,
                attempt_number=job.attempt_count,
                provider=generation.provider,
                started_at=now,
            )
            completed = GenerationAttempt.succeeded(
                attempt_id=started.attempt_id,
                job_id=started.job_id,
                attempt_number=started.attempt_number,
                provider=generation.provider,
                started_at=started.started_at,
                completed_at=now,
                provider_operation_id=generation.provider_operation_id,
            )
            job.succeed()
            tx.attempts.add(started)
            tx.attempts.complete(completed)
            tx.jobs.save(job)
            tx.append_event(JobEvent(
                event_id=f"{job.job_id}:reconciled:{job.attempt_count}",
                job_id=job.job_id,
                event_type=JobEventType.RECONCILED,
                occurred_at=now,
                attempt_number=job.attempt_count,
                metadata=(("provider", generation.provider),),
            ))
            tx.append_event(JobEvent(
                event_id=f"{job.job_id}:succeeded:{job.attempt_count}",
                job_id=job.job_id,
                event_type=JobEventType.SUCCEEDED,
                occurred_at=now,
                attempt_number=job.attempt_count,
            ))
            tx.commit()
        except Exception:
            tx.rollback()
            raise

    def _persist_reconciliation_required(
        self,
        message: QueueMessage,
        lease_token: str,
        now: datetime,
        provider: str,
    ) -> None:
        tx = self._transaction_factory()
        try:
            tx.assert_lease_owner(message.job_id, lease_token)
            job = tx.jobs.get(message.job_id)
            if job is None:
                raise KeyError(f"generation job not found: {message.job_id}")
            job.start()
            failure = Failure.from_code(
                "RECONCILIATION_REQUIRED",
                "external provider outcome is ambiguous and cannot be safely retried",
                provider=provider,
            )
            started = GenerationAttempt.started(
                attempt_id=f"{job.job_id}:attempt-{job.attempt_count}",
                job_id=job.job_id,
                attempt_number=job.attempt_count,
                provider=provider,
                started_at=now,
            )
            failed = GenerationAttempt.failed(
                attempt_id=started.attempt_id,
                job_id=started.job_id,
                attempt_number=started.attempt_number,
                provider=provider,
                started_at=started.started_at,
                completed_at=now,
                failure_code=failure.code.value,
            )
            job.fail(failure.code.value)
            tx.attempts.add(started)
            tx.attempts.complete(failed)
            tx.jobs.save(job)
            tx.append_event(JobEvent(
                event_id=f"{job.job_id}:failed:{job.attempt_count}",
                job_id=job.job_id,
                event_type=JobEventType.FAILED,
                occurred_at=now,
                attempt_number=job.attempt_count,
                failure_code=failure.code.value,
                metadata=(("provider", provider),),
            ))
            tx.commit()
        except Exception:
            tx.rollback()
            raise
