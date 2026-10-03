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
from app.application.persistence import ExecutionPersistenceTransaction
from app.application.worker import GenerationJobQueue, QueueMessage, WorkerLeaseRepository
from app.domain.events import JobEvent, JobEventType
from app.domain.generation import GenerationStatus


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
    ) -> None:
        if lease_duration <= timedelta(0):
            raise ValueError("lease_duration must be positive")
        self._queue = queue
        self._leases = leases
        self._transaction_factory = transaction_factory
        self._providers = providers
        self._lease_duration = lease_duration

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

            result = ExecuteGenerationJob(tx, self._providers).execute(
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
