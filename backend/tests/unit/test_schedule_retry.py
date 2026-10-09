from __future__ import annotations

from dataclasses import dataclass

import pytest

from app.application.retry import ScheduleGenerationRetry, ScheduleGenerationRetryCommand
from app.domain.events import JobEvent
from app.domain.generation import GenerationJob, RetryPolicy


class JobRepo:
    def __init__(self, job: GenerationJob) -> None:
        self.job = job

    def get(self, job_id: str) -> GenerationJob | None:
        return self.job if self.job.job_id == job_id else None


@dataclass
class Transaction:
    jobs: JobRepo
    events: list[JobEvent]
    commit_error: Exception | None = None
    rollback_calls: int = 0

    def append_event(self, event: JobEvent) -> None:
        self.events.append(event)

    def commit(self) -> None:
        if self.commit_error is not None:
            raise self.commit_error

    def rollback(self) -> None:
        self.rollback_calls += 1


def make_failed_job(error_code: str = "RATE_LIMITED") -> GenerationJob:
    job = GenerationJob.create("job-1", "video", "scene-1/v1")
    job.start()
    job.fail(error_code)
    return job


def test_retry_scheduling_moves_failed_job_to_retrying_and_emits_event() -> None:
    job = make_failed_job()
    tx = Transaction(JobRepo(job), [])
    service = ScheduleGenerationRetry(tx)

    result = service.execute(ScheduleGenerationRetryCommand("job-1", RetryPolicy(max_attempts=3)))

    assert result.scheduled is True
    assert result.job.status.value == "RETRYING"
    assert len(tx.events) == 1
    assert tx.events[0].event_type.value == "RETRY_SCHEDULED"
    assert tx.events[0].attempt_number == 1
    assert tx.events[0].failure_code == "RATE_LIMITED"


def test_non_retryable_failure_is_not_scheduled() -> None:
    job = make_failed_job("CONTENT_REJECTED")
    tx = Transaction(JobRepo(job), [])
    service = ScheduleGenerationRetry(tx)

    result = service.execute(ScheduleGenerationRetryCommand("job-1", RetryPolicy(max_attempts=3)))

    assert result.scheduled is False
    assert result.job.status.value == "FAILED"
    assert tx.events == []
    assert tx.rollback_calls == 1


def test_retry_limit_is_enforced() -> None:
    job = make_failed_job()
    job.attempt_count = 3
    tx = Transaction(JobRepo(job), [])
    service = ScheduleGenerationRetry(tx)

    result = service.execute(ScheduleGenerationRetryCommand("job-1", RetryPolicy(max_attempts=3)))

    assert result.scheduled is False
    assert result.job.status.value == "FAILED"
    assert tx.events == []


def test_retry_commit_failure_rolls_back() -> None:
    job = make_failed_job()
    tx = Transaction(JobRepo(job), [], commit_error=RuntimeError("commit failed"))
    service = ScheduleGenerationRetry(tx)

    with pytest.raises(RuntimeError, match="commit failed"):
        service.execute(ScheduleGenerationRetryCommand("job-1", RetryPolicy(max_attempts=3)))

    assert tx.rollback_calls == 1


def test_missing_job_is_rejected() -> None:
    job = GenerationJob.create("other-job", "video", "scene-1/v1")
    tx = Transaction(JobRepo(job), [])
    service = ScheduleGenerationRetry(tx)

    with pytest.raises(KeyError):
        service.execute(ScheduleGenerationRetryCommand("job-1", RetryPolicy()))

    assert tx.events == []
