from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum


class InvalidStateTransition(ValueError):
    pass


class GenerationStatus(str, Enum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    RETRYING = "RETRYING"
    CANCELLED = "CANCELLED"


_RETRYABLE_ERRORS = {
    "RATE_LIMITED",
    "TIMEOUT",
    "PROVIDER_FAILURE",
    "TRANSIENT_NETWORK",
}


def normalize_idempotency_key(value: str) -> str:
    normalized = "/".join(part.strip() for part in value.strip().lower().split("/"))
    if not normalized:
        raise ValueError("idempotency key cannot be blank")
    return normalized


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 3
    base_delay_seconds: int = 1

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        if self.base_delay_seconds < 0:
            raise ValueError("base_delay_seconds cannot be negative")

    def is_retryable(self, error_code: str, attempt_count: int) -> bool:
        return (
            error_code in _RETRYABLE_ERRORS
            and attempt_count < self.max_attempts
        )


class AttemptStatus(str, Enum):
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


@dataclass(frozen=True)
class GenerationAttempt:
    attempt_id: str
    job_id: str
    attempt_number: int
    provider: str
    started_at: datetime
    completed_at: datetime | None = None
    status: AttemptStatus = AttemptStatus.RUNNING
    provider_operation_id: str | None = None
    failure_code: str | None = None

    def __post_init__(self) -> None:
        if not self.attempt_id.strip():
            raise ValueError("attempt_id cannot be blank")
        if not self.job_id.strip():
            raise ValueError("job_id cannot be blank")
        if not self.provider.strip():
            raise ValueError("provider cannot be blank")
        if self.attempt_number < 1:
            raise ValueError("attempt_number must be at least 1")
        if self.completed_at is not None and self.completed_at < self.started_at:
            raise ValueError("completed_at cannot be before started_at")

    @classmethod
    def started(
        cls,
        attempt_id: str,
        job_id: str,
        attempt_number: int,
        provider: str,
        started_at: datetime,
    ) -> "GenerationAttempt":
        return cls(
            attempt_id=attempt_id,
            job_id=job_id,
            attempt_number=attempt_number,
            provider=provider,
            started_at=started_at,
        )

    @classmethod
    def succeeded(
        cls,
        attempt_id: str,
        job_id: str,
        attempt_number: int,
        provider: str,
        started_at: datetime,
        completed_at: datetime,
        provider_operation_id: str | None = None,
    ) -> "GenerationAttempt":
        return cls(
            attempt_id=attempt_id,
            job_id=job_id,
            attempt_number=attempt_number,
            provider=provider,
            started_at=started_at,
            completed_at=completed_at,
            status=AttemptStatus.SUCCEEDED,
            provider_operation_id=provider_operation_id,
        )

    @classmethod
    def failed(
        cls,
        attempt_id: str,
        job_id: str,
        attempt_number: int,
        provider: str,
        started_at: datetime,
        completed_at: datetime,
        failure_code: str,
        provider_operation_id: str | None = None,
    ) -> "GenerationAttempt":
        if not failure_code.strip():
            raise ValueError("failure_code cannot be blank")
        return cls(
            attempt_id=attempt_id,
            job_id=job_id,
            attempt_number=attempt_number,
            provider=provider,
            started_at=started_at,
            completed_at=completed_at,
            status=AttemptStatus.FAILED,
            provider_operation_id=provider_operation_id,
            failure_code=failure_code,
        )


@dataclass
class GenerationJob:
    job_id: str
    capability: str
    idempotency_key: str
    status: GenerationStatus = GenerationStatus.QUEUED
    attempt_count: int = 0
    failure_code: str | None = None

    @classmethod
    def create(
        cls,
        job_id: str,
        capability: str,
        idempotency_key: str,
    ) -> "GenerationJob":
        if not job_id.strip():
            raise ValueError("job_id cannot be blank")
        if not capability.strip():
            raise ValueError("capability cannot be blank")

        return cls(
            job_id=job_id,
            capability=capability,
            idempotency_key=normalize_idempotency_key(idempotency_key),
        )

    def start(self) -> None:
        if self.status not in {GenerationStatus.QUEUED, GenerationStatus.RETRYING}:
            raise InvalidStateTransition(
                f"cannot start generation from {self.status.value}"
            )
        self.status = GenerationStatus.RUNNING
        self.attempt_count += 1
        self.failure_code = None

    def succeed(self) -> None:
        if self.status is not GenerationStatus.RUNNING:
            raise InvalidStateTransition(
                f"cannot succeed generation from {self.status.value}"
            )
        self.status = GenerationStatus.SUCCEEDED

    def fail(self, error_code: str) -> None:
        if self.status is not GenerationStatus.RUNNING:
            raise InvalidStateTransition(
                f"cannot fail generation from {self.status.value}"
            )
        self.failure_code = error_code
        self.status = GenerationStatus.FAILED

    def schedule_retry(self, policy: RetryPolicy) -> bool:
        if self.status is not GenerationStatus.FAILED:
            raise InvalidStateTransition(
                f"cannot retry generation from {self.status.value}"
            )

        if self.failure_code is None:
            return False

        if not policy.is_retryable(self.failure_code, self.attempt_count):
            return False

        self.status = GenerationStatus.RETRYING
        return True
