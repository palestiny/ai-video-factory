from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from uuid import uuid4

from app.application.worker import QueueMessage, WorkerLease


@dataclass(frozen=True)
class QueuedMessage:
    message: QueueMessage
    available_at: datetime


class InMemoryGenerationJobQueue:
    """Deterministic queue double; delivery is intentionally at-least-once."""

    def __init__(self) -> None:
        self._messages: dict[str, QueuedMessage] = {}
        self._acked: set[str] = set()

    def enqueue(self, job_id: str) -> QueueMessage:
        return self.enqueue_after(job_id, timedelta(0))

    def enqueue_after(self, job_id: str, delay: timedelta) -> QueueMessage:
        if not job_id.strip():
            raise ValueError("job_id cannot be blank")
        if delay < timedelta(0):
            raise ValueError("delay cannot be negative")
        message = QueueMessage(message_id=str(uuid4()), job_id=job_id)
        self._messages[message.message_id] = QueuedMessage(message, datetime.now().astimezone() + delay)
        return message

    def ack(self, message: QueueMessage) -> None:
        self._acked.add(message.message_id)

    def release_or_requeue(self, message: QueueMessage) -> None:
        if message.message_id in self._acked:
            return
        redelivery = QueueMessage(
            message_id=str(uuid4()),
            job_id=message.job_id,
            delivery_attempt=message.delivery_attempt + 1,
        )
        self._messages[redelivery.message_id] = QueuedMessage(
            redelivery,
            datetime.now().astimezone(),
        )

    def is_acked(self, message_id: str) -> bool:
        return message_id in self._acked

    def pending(self) -> tuple[QueueMessage, ...]:
        return tuple(
            item.message
            for item in self._messages.values()
            if item.message.message_id not in self._acked
        )


class InMemoryWorkerLeaseRepository:
    """Deterministic lease store; claim is single-owner by job."""

    def __init__(self) -> None:
        self._leases: dict[str, WorkerLease] = {}

    def claim(
        self,
        job_id: str,
        worker_id: str,
        now: datetime,
        lease_duration: timedelta,
    ) -> WorkerLease | None:
        if lease_duration <= timedelta(0):
            raise ValueError("lease_duration must be positive")
        current = self._leases.get(job_id)
        if current is not None and current.expires_at > now:
            return None

        lease = WorkerLease(
            job_id=job_id,
            worker_id=worker_id,
            lease_token=str(uuid4()),
            acquired_at=now,
            expires_at=now + lease_duration,
        )
        self._leases[job_id] = lease
        return lease

    def renew(
        self,
        job_id: str,
        lease_token: str,
        now: datetime,
        lease_duration: timedelta,
    ) -> WorkerLease | None:
        if lease_duration <= timedelta(0):
            raise ValueError("lease_duration must be positive")
        current = self._leases.get(job_id)
        if (
            current is None
            or current.lease_token != lease_token
            or current.expires_at <= now
        ):
            return None

        renewed = WorkerLease(
            job_id=job_id,
            worker_id=current.worker_id,
            lease_token=current.lease_token,
            acquired_at=current.acquired_at,
            expires_at=now + lease_duration,
        )
        self._leases[job_id] = renewed
        return renewed

    def release(self, job_id: str, lease_token: str) -> bool:
        current = self._leases.get(job_id)
        if current is None or current.lease_token != lease_token:
            return False
        del self._leases[job_id]
        return True

    def recover_expired(self, job_id: str, now: datetime) -> bool:
        current = self._leases.get(job_id)
        if current is None or current.expires_at > now:
            return False
        del self._leases[job_id]
        return True

    def current(self, job_id: str) -> WorkerLease | None:
        return self._leases.get(job_id)
