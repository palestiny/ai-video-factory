from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum


class JobEventType(str, Enum):
    CREATED = "CREATED"
    STARTED = "STARTED"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    RETRY_SCHEDULED = "RETRY_SCHEDULED"
    CANCELLED = "CANCELLED"
    RECOVERED = "RECOVERED"


@dataclass(frozen=True)
class JobEvent:
    event_id: str
    job_id: str
    event_type: JobEventType
    occurred_at: datetime
    attempt_number: int | None = None
    failure_code: str | None = None
    metadata: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        if not self.event_id.strip():
            raise ValueError("event_id cannot be blank")
        if not self.job_id.strip():
            raise ValueError("job_id cannot be blank")
        if self.attempt_number is not None and self.attempt_number < 1:
            raise ValueError("attempt_number must be at least 1")
        if self.failure_code is not None and not self.failure_code.strip():
            raise ValueError("failure_code cannot be blank")
