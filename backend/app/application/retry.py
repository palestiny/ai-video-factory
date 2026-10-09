from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Protocol

from app.domain.events import JobEvent, JobEventType
from app.domain.generation import GenerationJob, RetryPolicy


class RetryJobRepository(Protocol):
    def get(self, job_id: str) -> GenerationJob | None:
        ...


class RetryTransaction(Protocol):
    jobs: RetryJobRepository

    def append_event(self, event: JobEvent) -> None:
        ...

    def commit(self) -> None:
        ...

    def rollback(self) -> None:
        ...


@dataclass(frozen=True)
class ScheduleGenerationRetryCommand:
    job_id: str
    policy: RetryPolicy


@dataclass(frozen=True)
class ScheduleGenerationRetryResult:
    job: GenerationJob
    scheduled: bool


class ScheduleGenerationRetry:
    """Move a failed job to RETRYING; queue/backoff dispatch stays outside."""

    def __init__(self, transaction: RetryTransaction) -> None:
        self._transaction = transaction

    def execute(self, command: ScheduleGenerationRetryCommand) -> ScheduleGenerationRetryResult:
        job = self._transaction.jobs.get(command.job_id)
        if job is None:
            raise KeyError(f"generation job not found: {command.job_id}")

        scheduled = job.schedule_retry(command.policy)
        if not scheduled:
            self._transaction.rollback()
            return ScheduleGenerationRetryResult(job=job, scheduled=False)

        occurred_at = datetime.now(timezone.utc)
        self._transaction.append_event(
            JobEvent(
                event_id=f"{job.job_id}:retry-scheduled:{job.attempt_count}",
                job_id=job.job_id,
                event_type=JobEventType.RETRY_SCHEDULED,
                occurred_at=occurred_at,
                attempt_number=job.attempt_count,
                failure_code=job.failure_code,
            )
        )
        try:
            self._transaction.commit()
        except Exception:
            self._transaction.rollback()
            raise

        return ScheduleGenerationRetryResult(job=job, scheduled=True)
