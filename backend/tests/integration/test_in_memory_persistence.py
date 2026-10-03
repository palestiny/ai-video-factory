from datetime import datetime, timezone

import pytest

from app.application.idempotency import IdempotencyConflict
from app.application.submission import SubmitGenerationCommand, SubmitGenerationJob
from app.domain.events import JobEvent, JobEventType
from app.domain.generation import GenerationAttempt, GenerationJob
from app.infrastructure.in_memory_persistence import InMemoryPersistenceTransaction


def test_submission_commit_persists_job_event_and_idempotency() -> None:
    tx = InMemoryPersistenceTransaction()
    result = SubmitGenerationJob(tx).execute(
        SubmitGenerationCommand(
            job_id="job-1",
            capability="video",
            idempotency_key=" Video / Scene-1 ",
            request_fingerprint=" request-a ",
        )
    )

    assert result.created is True
    assert tx.commit_count == 1
    assert tx.jobs.get("job-1") is not None
    assert tx.events[0].event_type is JobEventType.CREATED

    duplicate = SubmitGenerationJob(tx).execute(
        SubmitGenerationCommand(
            job_id="other-id",
            capability="video",
            idempotency_key="video/scene-1",
            request_fingerprint="request-a",
        )
    )
    assert duplicate.created is False
    assert duplicate.job.job_id == "job-1"


def test_submission_commit_failure_rolls_back_job_event_and_reservation() -> None:
    tx = InMemoryPersistenceTransaction()
    tx.fail_commit = True

    with pytest.raises(RuntimeError, match="commit failed"):
        SubmitGenerationJob(tx).execute(
            SubmitGenerationCommand(
                job_id="job-1",
                capability="video",
                idempotency_key="video/scene-1",
                request_fingerprint="request-a",
            )
        )

    assert tx.jobs.get("job-1") is None
    assert tx.events == ()

    clean = InMemoryPersistenceTransaction(
        jobs=tx._jobs,
        attempts=tx._attempts,
        events=list(tx.events),
        idempotency=tx.idempotency,
    )
    result = SubmitGenerationJob(clean).execute(
        SubmitGenerationCommand(
            job_id="job-2",
            capability="video",
            idempotency_key="video/scene-1",
            request_fingerprint="request-a",
        )
    )
    assert result.created is True


def test_conflicting_idempotency_fingerprint_is_rejected() -> None:
    tx = InMemoryPersistenceTransaction()
    SubmitGenerationJob(tx).execute(
        SubmitGenerationCommand(
            job_id="job-1",
            capability="video",
            idempotency_key="video/scene-1",
            request_fingerprint="request-a",
        )
    )

    with pytest.raises(IdempotencyConflict):
        SubmitGenerationJob(tx).execute(
            SubmitGenerationCommand(
                job_id="job-2",
                capability="video",
                idempotency_key="video/scene-1",
                request_fingerprint="request-b",
            )
        )


def test_attempt_completion_preserves_immutable_history() -> None:
    tx = InMemoryPersistenceTransaction()
    started_at = datetime.now(timezone.utc)
    started = GenerationAttempt.started(
        attempt_id="job-1:attempt-1",
        job_id="job-1",
        attempt_number=1,
        provider="fake",
        started_at=started_at,
    )
    completed = GenerationAttempt.succeeded(
        attempt_id=started.attempt_id,
        job_id=started.job_id,
        attempt_number=started.attempt_number,
        provider=started.provider,
        started_at=started.started_at,
        completed_at=datetime.now(timezone.utc),
    )

    tx.attempts.add(started)
    tx.attempts.complete(completed)

    assert tx._attempts[started.attempt_id] == [started, completed]


def test_rollback_restores_attempts_and_events_to_transaction_snapshot() -> None:
    tx = InMemoryPersistenceTransaction()
    job = GenerationJob.create("job-1", "video", "video/scene-1")
    tx.jobs.add(job)
    tx.append_event(
        JobEvent(
            event_id="job-1:created",
            job_id="job-1",
            event_type=JobEventType.CREATED,
            occurred_at=datetime.now(timezone.utc),
        )
    )
    tx.commit()

    attempt = GenerationAttempt.started(
        attempt_id="job-1:attempt-1",
        job_id="job-1",
        attempt_number=1,
        provider="fake",
        started_at=datetime.now(timezone.utc),
    )
    tx.attempts.add(attempt)
    tx.append_event(
        JobEvent(
            event_id="job-1:started:1",
            job_id="job-1",
            event_type=JobEventType.STARTED,
            occurred_at=datetime.now(timezone.utc),
            attempt_number=1,
        )
    )

    tx.rollback()

    assert tx.attempts._items == {}
    assert [event.event_id for event in tx.events] == ["job-1:created"]
