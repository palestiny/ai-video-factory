from __future__ import annotations

from datetime import datetime, timedelta
from typing import Callable, Protocol

from app.application.persistence import ExecutionPersistenceTransaction
from app.application.ports import GenerationRequest, GenerationResult, ProviderOperationPort
from app.application.provider_operation import ProviderOperation, ProviderOperationStatus
from app.application.provider_operation_lifecycle import (
    ProviderOperationLifecycle,
    ProviderOperationPersistenceUncertain,
)
from app.application.reconciliation import ProviderExecutionContract, ProviderOperationSafety
from app.application.worker import GenerationJobQueue, QueueMessage, WorkerLeaseRepository
from app.application.worker_execution import WorkerDeliveryResult, WorkerDeliveryStatus
from app.domain.events import JobEvent, JobEventType
from app.domain.failure import Failure
from app.domain.generation import AttemptStatus, GenerationAttempt, GenerationStatus


class AsyncProviderResolver(Protocol):
    def resolve(self, capability: str) -> ProviderOperationPort:
        ...


class AsyncProviderContractResolver(Protocol):
    def resolve_contract(self, provider_name: str) -> ProviderExecutionContract:
        ...


class ExecuteAsyncProviderDelivery:
    """Handle one short delivery for submit/poll/finalize of an async operation.

    The durable provider operation is the recovery anchor between deliveries.
    A non-terminal operation schedules another message and releases the lease;
    no lease is held while the provider performs the long-running generation.
    """

    _TERMINAL = {
        ProviderOperationStatus.SUCCEEDED,
        ProviderOperationStatus.FAILED,
        ProviderOperationStatus.CANCELLED,
    }

    def __init__(
        self,
        queue: GenerationJobQueue,
        leases: WorkerLeaseRepository,
        transaction_factory: Callable[[], ExecutionPersistenceTransaction],
        providers: AsyncProviderResolver,
        contracts: AsyncProviderContractResolver,
        lease_duration: timedelta,
        poll_delay: timedelta = timedelta(seconds=5),
    ) -> None:
        if lease_duration <= timedelta(0):
            raise ValueError("lease_duration must be positive")
        if poll_delay < timedelta(0):
            raise ValueError("poll_delay cannot be negative")
        self._queue = queue
        self._leases = leases
        self._transaction_factory = transaction_factory
        self._providers = providers
        self._contracts = contracts
        self._lease_duration = lease_duration
        self._poll_delay = poll_delay

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
            message.job_id, worker_id, now, self._lease_duration
        )
        if lease is None:
            return WorkerDeliveryResult(
                WorkerDeliveryStatus.NOT_CLAIMED, message.message_id,
                message.job_id, "active lease held by another worker",
            )

        transactions: list[ExecutionPersistenceTransaction] = []

        def new_transaction() -> ExecutionPersistenceTransaction:
            transaction = self._transaction_factory()
            transactions.append(transaction)
            return transaction

        try:
            tx = new_transaction()
            tx.assert_lease_owner(message.job_id, lease.lease_token)
            job = tx.jobs.get(message.job_id)
            if job is None:
                self._queue.ack(message)
                return self._result(message, WorkerDeliveryStatus.ACKED, "job no longer exists")
            if job.status is GenerationStatus.CANCELLED:
                # Cancellation may have committed between queue deliveries, so
                # close any active attempt before acknowledging the stale message.
                attempt_id = f"{job.job_id}:attempt-{job.attempt_count}"
                history = tx.attempts.history(attempt_id)
                if history and history[-1].status is AttemptStatus.RUNNING:
                    active = history[-1]
                    operation = tx.provider_operations.get_by_idempotency_key(
                        active.provider, job.idempotency_key
                    )
                    tx.attempts.complete(GenerationAttempt.cancelled(
                        attempt_id=active.attempt_id,
                        job_id=active.job_id,
                        attempt_number=active.attempt_number,
                        provider=active.provider,
                        started_at=active.started_at,
                        completed_at=now,
                        provider_operation_id=(
                            operation.operation_id if operation is not None else None
                        ),
                    ))
                    try:
                        tx.commit()
                    except Exception:
                        tx.rollback()
                        self._queue.release_or_requeue(message)
                        return self._result(
                            message, WorkerDeliveryStatus.REQUEUED,
                            "cancelled job attempt finalization failed; delivery will retry",
                        )
                self._queue.ack(message)
                return self._result(
                    message, WorkerDeliveryStatus.ACKED, "job already terminal: CANCELLED"
                )
            if job.status is GenerationStatus.SUCCEEDED:
                self._queue.ack(message)
                return self._result(message, WorkerDeliveryStatus.ACKED, "job already terminal: SUCCEEDED")

            provider = self._providers.resolve(job.capability)
            provider_name = provider.provider_name
            contract = self._contracts.resolve_contract(provider_name)
            if contract.provider != provider_name:
                raise ValueError("provider contract/provider name mismatch")

            operation = tx.provider_operations.get_by_idempotency_key(
                provider_name, job.idempotency_key
            )
            if operation is None:
                # An existing RUNNING job without a durable operation is ambiguous:
                # the provider may have accepted the previous submit before a crash.
                if job.status is GenerationStatus.RUNNING:
                    if contract.operation_safety is ProviderOperationSafety.NON_RECONCILABLE:
                        attempt_id = f"{job.job_id}:attempt-{job.attempt_count}"
                        history = tx.attempts.history(attempt_id)
                        if not history or history[-1].status is not AttemptStatus.RUNNING:
                            raise RuntimeError("ambiguous running job has no active attempt")
                        failure_code = "RECONCILIATION_REQUIRED"
                        job.fail(failure_code)
                        tx.attempts.complete(GenerationAttempt.failed(
                            attempt_id=history[-1].attempt_id,
                            job_id=history[-1].job_id,
                            attempt_number=history[-1].attempt_number,
                            provider=provider_name,
                            started_at=history[-1].started_at,
                            completed_at=now,
                            failure_code=failure_code,
                        ))
                        tx.jobs.save(job)
                        tx.append_event(JobEvent(
                            event_id=f"{job.job_id}:failed:{job.attempt_count}",
                            job_id=job.job_id,
                            event_type=JobEventType.FAILED,
                            occurred_at=now,
                            attempt_number=job.attempt_count,
                            failure_code=failure_code,
                            metadata=(("provider", provider_name),),
                        ))
                        tx.commit()
                        self._queue.ack(message)
                        return self._result(
                            message, WorkerDeliveryStatus.ACKED,
                            "ambiguous non-reconcilable submission recorded for manual reconciliation",
                        )
                    if contract.operation_safety is ProviderOperationSafety.RECONCILABLE:
                        # This delivery has no durable provider identity. Do not submit
                        # again unless the adapter's reconciliation port explicitly
                        # proves that no operation exists.
                        reconciliation = getattr(provider, "reconciliation", None)
                        if reconciliation is None:
                            self._queue.release_or_requeue(message)
                            return self._result(
                                message, WorkerDeliveryStatus.REQUEUED,
                                "reconcilable provider has no reconciliation port configured",
                            )
                        from app.application.reconciliation import ReconciliationQuery
                        try:
                            reconciled = reconciliation.reconcile(
                                ReconciliationQuery(job.idempotency_key)
                            )
                        except Exception:
                            self._queue.release_or_requeue(message)
                            return self._result(
                                message, WorkerDeliveryStatus.REQUEUED,
                                "reconciliation failed; provider submission was not repeated",
                            )
                        if reconciled is not None:
                            if not isinstance(reconciled, GenerationResult):
                                raise ValueError("reconciliation returned an invalid generation result")
                            attempt_id = f"{job.job_id}:attempt-{job.attempt_count}"
                            history = tx.attempts.history(attempt_id)
                            if not history or history[-1].status is not AttemptStatus.RUNNING:
                                raise RuntimeError("reconciled job has no active attempt")
                            job.succeed()
                            tx.attempts.complete(GenerationAttempt.succeeded(
                                attempt_id=history[-1].attempt_id,
                                job_id=history[-1].job_id,
                                attempt_number=history[-1].attempt_number,
                                provider=reconciled.provider,
                                started_at=history[-1].started_at,
                                completed_at=now,
                                provider_operation_id=reconciled.provider_operation_id,
                            ))
                            tx.jobs.save(job)
                            tx.append_event(JobEvent(
                                event_id=f"{job.job_id}:reconciled:{job.attempt_count}",
                                job_id=job.job_id,
                                event_type=JobEventType.RECONCILED,
                                occurred_at=now,
                                attempt_number=job.attempt_count,
                                metadata=(("provider", provider_name),),
                            ))
                            tx.append_event(JobEvent(
                                event_id=f"{job.job_id}:succeeded:{job.attempt_count}",
                                job_id=job.job_id,
                                event_type=JobEventType.SUCCEEDED,
                                occurred_at=now,
                                attempt_number=job.attempt_count,
                            ))
                            tx.commit()
                            self._queue.ack(message)
                            return self._result(
                                message, WorkerDeliveryStatus.ACKED,
                                "existing provider result reconciled without resubmission",
                            )
                    attempt_number = job.attempt_count
                    history = tx.attempts.history(f"{job.job_id}:attempt-{attempt_number}")
                    if not history:
                        raise RuntimeError("running job has no durable attempt history")
                else:
                    job.start()
                    started_at = now
                    attempt = GenerationAttempt.started(
                        attempt_id=f"{job.job_id}:attempt-{job.attempt_count}",
                        job_id=job.job_id,
                        attempt_number=job.attempt_count,
                        provider=provider_name,
                        started_at=started_at,
                    )
                    tx.attempts.add(attempt)
                    tx.jobs.save(job)
                    tx.append_event(JobEvent(
                        event_id=f"{job.job_id}:started:{job.attempt_count}",
                        job_id=job.job_id,
                        event_type=JobEventType.STARTED,
                        occurred_at=now,
                        attempt_number=job.attempt_count,
                    ))
                    tx.commit()

                request = GenerationRequest(
                    job_id=job.job_id,
                    capability=job.capability,
                    inputs=inputs,
                    references=references,
                    constraints=constraints or {},
                    idempotency_key=job.idempotency_key,
                )
                lifecycle = ProviderOperationLifecycle(
                    tx, provider, provider_name, contract
                )
                try:
                    operation = lifecycle.submit(request)
                except Exception:
                    self._queue.release_or_requeue(message)
                    return self._result(
                        message, WorkerDeliveryStatus.REQUEUED,
                        "provider submission or operation persistence is uncertain",
                    )
            else:
                lifecycle = ProviderOperationLifecycle(
                    tx, provider, provider_name, contract
                )

            if operation.status not in self._TERMINAL:
                try:
                    expected_generation = (
                        message.generation
                        if message.intent_kind == "PROVIDER_POLL"
                        else 0
                    )
                    observed = lifecycle.poll(
                        operation,
                        next_poll_due_at=now + self._poll_delay,
                        expected_poll_generation=expected_generation,
                        job_id=job.job_id,
                    )
                except ProviderOperationPersistenceUncertain:
                    self._queue.release_or_requeue(message)
                    return self._result(
                        message, WorkerDeliveryStatus.REQUEUED,
                        "provider status observed but durable status is uncertain",
                    )
                if observed.status not in self._TERMINAL:
                    # The lifecycle committed the next poll intent in the same
                    # transaction as the provider-operation observation. ACK only
                    # after that atomic commit has returned successfully.
                    self._queue.ack(message)
                    return self._result(
                        message, WorkerDeliveryStatus.ACKED,
                        "provider operation persisted; next poll scheduled",
                    )
                operation = observed.operation
                terminal_result = observed.result
                failure_code = observed.failure_code
            else:
                terminal_result = operation.terminal_result
                failure_code = operation.failure_code

            # Re-read authoritative job state immediately before finalization so a
            # cancellation committed during provider execution cannot be resurrected.
            finalize_tx = new_transaction()
            finalize_tx.assert_lease_owner(message.job_id, lease.lease_token)
            current_job = finalize_tx.jobs.get(message.job_id)
            if current_job is None:
                self._queue.ack(message)
                return self._result(
                    message, WorkerDeliveryStatus.ACKED,
                    "late provider outcome ignored because job is missing",
                )
            if current_job.status is GenerationStatus.CANCELLED:
                # Cancellation owns the job outcome, but the provider attempt must
                # still become terminal so it does not remain RUNNING forever.
                attempt_id = f"{current_job.job_id}:attempt-{current_job.attempt_count}"
                history = finalize_tx.attempts.history(attempt_id)
                if history and history[-1].status is AttemptStatus.RUNNING:
                    started_attempt = history[-1]
                    finalize_tx.attempts.complete(GenerationAttempt.cancelled(
                        attempt_id=started_attempt.attempt_id,
                        job_id=started_attempt.job_id,
                        attempt_number=started_attempt.attempt_number,
                        provider=provider_name,
                        started_at=started_attempt.started_at,
                        completed_at=now,
                        provider_operation_id=operation.operation_id,
                    ))
                    try:
                        finalize_tx.commit()
                    except Exception:
                        finalize_tx.rollback()
                        self._queue.release_or_requeue(message)
                        return self._result(
                            message, WorkerDeliveryStatus.REQUEUED,
                            "cancelled job attempt finalization failed; redelivery will retry",
                        )
                self._queue.ack(message)
                return self._result(
                    message, WorkerDeliveryStatus.ACKED,
                    "late provider outcome ignored; cancelled job attempt terminalized",
                )
            if current_job.status is GenerationStatus.SUCCEEDED:
                self._queue.ack(message)
                return self._result(message, WorkerDeliveryStatus.ACKED, "job already completed")

            attempt_id = f"{current_job.job_id}:attempt-{current_job.attempt_count}"
            history = finalize_tx.attempts.history(attempt_id)
            if not history:
                raise RuntimeError(f"attempt history missing: {attempt_id}")
            started_attempt = history[-1]
            if started_attempt.status is not AttemptStatus.RUNNING:
                self._queue.ack(message)
                return self._result(message, WorkerDeliveryStatus.ACKED, "attempt already terminal")

            completed_at = now
            if operation.status is ProviderOperationStatus.SUCCEEDED:
                if not isinstance(terminal_result, GenerationResult):
                    raise ValueError("successful provider operation has no normalized generation result")
                current_job.succeed()
                completed = GenerationAttempt.succeeded(
                    attempt_id=started_attempt.attempt_id,
                    job_id=started_attempt.job_id,
                    attempt_number=started_attempt.attempt_number,
                    provider=provider_name,
                    started_at=started_attempt.started_at,
                    completed_at=completed_at,
                    provider_operation_id=operation.operation_id,
                )
                event_type = JobEventType.SUCCEEDED
                event_failure = None
            else:
                code = failure_code or (
                    "PROVIDER_CANCELLED"
                    if operation.status is ProviderOperationStatus.CANCELLED
                    else "PROVIDER_FAILURE"
                )
                current_job.fail(code)
                completed = GenerationAttempt.failed(
                    attempt_id=started_attempt.attempt_id,
                    job_id=started_attempt.job_id,
                    attempt_number=started_attempt.attempt_number,
                    provider=provider_name,
                    started_at=started_attempt.started_at,
                    completed_at=completed_at,
                    failure_code=code,
                    provider_operation_id=operation.operation_id,
                )
                event_type = JobEventType.FAILED
                event_failure = code

            finalize_tx.attempts.complete(completed)
            finalize_tx.jobs.save(current_job)
            finalize_tx.append_event(JobEvent(
                event_id=f"{current_job.job_id}:{event_type.value.lower()}:{current_job.attempt_count}",
                job_id=current_job.job_id,
                event_type=event_type,
                occurred_at=completed_at,
                attempt_number=current_job.attempt_count,
                failure_code=event_failure,
                metadata=(("provider", provider_name), ("provider_operation_id", operation.operation_id)),
            ))
            try:
                finalize_tx.commit()
            except Exception:
                finalize_tx.rollback()
                self._queue.release_or_requeue(message)
                return self._result(
                    message, WorkerDeliveryStatus.REQUEUED,
                    "terminal operation is durable but job finalization failed; replay will reuse operation",
                )
            self._queue.ack(message)
            return self._result(
                message, WorkerDeliveryStatus.ACKED,
                "terminal job outcome committed before acknowledgement",
            )
        except Exception:
            self._queue.release_or_requeue(message)
            return self._result(
                message, WorkerDeliveryStatus.REQUEUED,
                "async provider delivery failed; durable operation will be replayed",
            )
        finally:
            try:
                for transaction in reversed(transactions):
                    if hasattr(transaction, "close"):
                        transaction.close()
            finally:
                self._leases.release(message.job_id, lease.lease_token)

    @staticmethod
    def _result(
        message: QueueMessage,
        status: WorkerDeliveryStatus,
        reason: str,
    ) -> WorkerDeliveryResult:
        return WorkerDeliveryResult(status, message.message_id, message.job_id, reason)
