from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Protocol


@dataclass(frozen=True)
class QueueMessage:
    message_id: str
    job_id: str
    delivery_attempt: int = 1
    claim_token: str | None = None
    intent_key: str | None = None
    intent_kind: str = "JOB_EXECUTION"
    provider: str | None = None
    operation_id: str | None = None
    generation: int | None = None

    def __post_init__(self) -> None:
        if not self.message_id.strip():
            raise ValueError("message_id cannot be blank")
        if not self.job_id.strip():
            raise ValueError("job_id cannot be blank")
        if self.delivery_attempt < 1:
            raise ValueError("delivery_attempt must be >= 1")
        if self.intent_kind == "PROVIDER_POLL":
            if not self.intent_key or not self.provider or not self.operation_id:
                raise ValueError("provider poll delivery requires intent and operation identity")
            if self.generation is None or self.generation < 1:
                raise ValueError("provider poll delivery generation must be >= 1")
        elif any(value is not None for value in (self.provider, self.operation_id, self.generation)):
            raise ValueError("non-poll delivery cannot carry provider poll identity")


class GenerationJobQueue(Protocol):
    def enqueue(self, job_id: str) -> QueueMessage:
        ...

    def enqueue_after(self, job_id: str, delay: timedelta) -> QueueMessage:
        ...

    def claim_next(
        self,
        worker_id: str,
        now: datetime,
        lease_duration: timedelta,
    ) -> QueueMessage | None:
        """Claim one due message; expired claims may be reclaimed with a new token."""
        ...

    def ack(self, message: QueueMessage) -> None:
        ...

    def release_or_requeue(self, message: QueueMessage) -> None:
        ...


@dataclass(frozen=True)
class WorkerLease:
    job_id: str
    worker_id: str
    lease_token: str
    acquired_at: datetime
    expires_at: datetime

    def __post_init__(self) -> None:
        if not self.job_id.strip():
            raise ValueError("job_id cannot be blank")
        if not self.worker_id.strip():
            raise ValueError("worker_id cannot be blank")
        if not self.lease_token.strip():
            raise ValueError("lease_token cannot be blank")
        if self.expires_at <= self.acquired_at:
            raise ValueError("expires_at must be after acquired_at")


class WorkerLeaseRepository(Protocol):
    def claim(
        self,
        job_id: str,
        worker_id: str,
        now: datetime,
        lease_duration: timedelta,
    ) -> WorkerLease | None:
        ...

    def renew(
        self,
        job_id: str,
        lease_token: str,
        now: datetime,
        lease_duration: timedelta,
    ) -> WorkerLease | None:
        ...

    def release(self, job_id: str, lease_token: str) -> bool:
        ...

    def recover_expired(
        self,
        job_id: str,
        now: datetime,
    ) -> bool:
        ...
