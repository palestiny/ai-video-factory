from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

import pytest

from app.application.idempotency import (
    IdempotencyConflict,
    InMemoryIdempotencyRepository,
)
from app.application.submission import (
    SubmitGenerationCommand,
    SubmitGenerationJob,
)
from app.domain.events import JobEvent
from app.domain.generation import GenerationJob


@dataclass
class InMemoryTransaction:
    idempotency: InMemoryIdempotencyRepository
    jobs: dict[str, GenerationJob]
    events: list[JobEvent]
    fail_on_commit: bool = False

    def __post_init__(self) -> None:
        self._committed = False
        self._snapshot = (
            dict(self.jobs),
            dict(self.idempotency._reservations or {}),
            list(self.events),
        )

    def add(self, job: GenerationJob) -> None:
        self.jobs[job.job_id] = job

    def get(self, job_id: str) -> GenerationJob | None:
        return self.jobs.get(job_id)

    def append_event(self, event: JobEvent) -> None:
        self.events.append(event)

    def commit(self) -> None:
        if self.fail_on_commit:
            self.rollback()
            raise RuntimeError("commit failed")
        self._committed = True

    def rollback(self) -> None:
        if self._committed:
            return
        jobs, reservations, events = self._snapshot
        self.jobs.clear()
        self.jobs.update(jobs)
        self.idempotency._reservations = reservations
        self.events[:] = events


class TransactionAdapter:
    def __init__(self, tx: InMemoryTransaction) -> None:
        self.idempotency = tx.idempotency
        self.jobs = _JobRepository(tx)
        self._tx = tx

    def append_event(self, event: JobEvent) -> None:
        self._tx.append_event(event)

    def commit(self) -> None:
        self._tx.commit()

    def rollback(self) -> None:
        self._tx.rollback()


class _JobRepository:
    def __init__(self, tx: InMemoryTransaction) -> None:
        self._tx = tx

    def add(self, job: GenerationJob) -> None:
        self._tx.add(job)

    def get(self, job_id: str) -> GenerationJob | None:
        return self._tx.get(job_id)


def command(**overrides: str) -> SubmitGenerationCommand:
    values = {
        "job_id": "job-1",
        "capability": "video",
        "idempotency_key": "scene-1/v1",
        "request_fingerprint": "fingerprint-1",
    }
    values.update(overrides)
    return SubmitGenerationCommand(**values)


def make_service(
    *,
    fail_on_commit: bool = False,
) -> tuple[SubmitGenerationJob, InMemoryTransaction]:
    tx = InMemoryTransaction(InMemoryIdempotencyRepository(), {}, [])
    tx.fail_on_commit = fail_on_commit
    return SubmitGenerationJob(TransactionAdapter(tx)), tx


def test_first_submission_creates_job_and_created_event() -> None:
    service, tx = make_service()

    result = service.execute(command())

    assert result.created is True
    assert result.job.status.value == "QUEUED"
    assert tx.jobs["job-1"] == result.job
    assert len(tx.events) == 1
    assert tx.events[0].job_id == "job-1"


def test_duplicate_equivalent_submission_returns_existing_job_without_new_event() -> None:
    service, tx = make_service()

    first = service.execute(command())
    second = service.execute(command(job_id="job-2"))

    assert second.created is False
    assert second.job.job_id == first.job.job_id
    assert list(tx.jobs) == ["job-1"]
    assert len(tx.events) == 1


def test_same_key_with_different_fingerprint_conflicts() -> None:
    service, _ = make_service()

    service.execute(command())

    with pytest.raises(IdempotencyConflict):
        service.execute(command(job_id="job-2", request_fingerprint="different"))


def test_commit_failure_rolls_back_job_reservation_and_event() -> None:
    service, tx = make_service(fail_on_commit=True)

    with pytest.raises(RuntimeError, match="commit failed"):
        service.execute(command())

    assert tx.jobs == {}
    assert tx.events == []
    assert tx.idempotency._reservations == {}


def test_orphaned_reservation_is_detected_and_rolled_back() -> None:
    service, tx = make_service()
    tx.idempotency.reserve(
        key="scene-1/v1",
        request_fingerprint="fingerprint-1",
        job_id="missing-job",
    )

    with pytest.raises(RuntimeError, match="missing generation job"):
        service.execute(command(job_id="new-job"))

    assert tx.events == []
    assert tx.jobs == {}


def test_event_contract_uses_utc_timestamp() -> None:
    service, tx = make_service()

    service.execute(command())

    assert tx.events[0].occurred_at.tzinfo == timezone.utc
    assert isinstance(tx.events[0], JobEvent)
    assert isinstance(tx.events[0].occurred_at, datetime)
