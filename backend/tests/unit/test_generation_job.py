import pytest

from app.domain.generation import (
    GenerationJob,
    GenerationStatus,
    InvalidStateTransition,
    RetryPolicy,
    normalize_idempotency_key,
)


def test_new_job_starts_queued():
    job = GenerationJob.create(
        job_id="job-1",
        capability="video",
        idempotency_key="video:scene-1:v1",
    )
    assert job.status is GenerationStatus.QUEUED
    assert job.attempt_count == 0


def test_valid_lifecycle_to_succeeded():
    job = GenerationJob.create("job-1", "video", "scene-1")
    job.start()
    job.succeed()
    assert job.status is GenerationStatus.SUCCEEDED


def test_failed_job_can_retry():
    job = GenerationJob.create("job-1", "video", "scene-1")
    job.start()
    job.fail("TIMEOUT")
    assert job.status is GenerationStatus.FAILED

    assert job.schedule_retry(RetryPolicy(max_attempts=3, base_delay_seconds=2))
    assert job.status is GenerationStatus.RETRYING
    assert job.attempt_count == 1


def test_non_retryable_failure_cannot_retry():
    job = GenerationJob.create("job-1", "video", "scene-1")
    job.start()
    job.fail("CONTENT_REJECTED")

    assert not job.schedule_retry(RetryPolicy(max_attempts=3))
    assert job.status is GenerationStatus.FAILED


def test_retry_limit_is_enforced():
    job = GenerationJob.create("job-1", "video", "scene-1")
    policy = RetryPolicy(max_attempts=2)

    job.start()
    job.fail("TIMEOUT")
    assert job.schedule_retry(policy)

    job.start()
    job.fail("TIMEOUT")
    assert not job.schedule_retry(policy)
    assert job.status is GenerationStatus.FAILED


def test_completed_job_cannot_restart():
    job = GenerationJob.create("job-1", "video", "scene-1")
    job.start()
    job.succeed()

    with pytest.raises(InvalidStateTransition):
        job.start()


def test_idempotency_normalization_is_deterministic():
    assert normalize_idempotency_key("  Scene-1 / V1 ") == "scene-1/v1"


def test_blank_idempotency_key_is_rejected():
    with pytest.raises(ValueError):
        normalize_idempotency_key("   ")


def test_generation_attempt_is_immutable_and_starts_running():
    from datetime import datetime, timezone

    from app.domain.generation import AttemptStatus, GenerationAttempt

    started_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    attempt = GenerationAttempt.started(
        attempt_id="attempt-1",
        job_id="job-1",
        attempt_number=1,
        provider="fake",
        started_at=started_at,
    )

    assert attempt.status is AttemptStatus.RUNNING
    assert attempt.attempt_number == 1

    with pytest.raises(Exception):
        attempt.status = AttemptStatus.SUCCEEDED


def test_generation_attempt_terminal_success_is_immutable():
    from datetime import datetime, timezone

    from app.domain.generation import AttemptStatus, GenerationAttempt

    started_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    completed_at = datetime(2026, 1, 1, 0, 1, tzinfo=timezone.utc)

    attempt = GenerationAttempt.succeeded(
        attempt_id="attempt-1",
        job_id="job-1",
        attempt_number=1,
        provider="fake",
        started_at=started_at,
        completed_at=completed_at,
        provider_operation_id="op-1",
    )

    assert attempt.status is AttemptStatus.SUCCEEDED
    assert attempt.completed_at == completed_at
    assert attempt.failure_code is None


def test_generation_attempt_terminal_failure_records_normalized_failure():
    from datetime import datetime, timezone

    from app.domain.generation import AttemptStatus, GenerationAttempt

    started_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    completed_at = datetime(2026, 1, 1, 0, 1, tzinfo=timezone.utc)

    attempt = GenerationAttempt.failed(
        attempt_id="attempt-1",
        job_id="job-1",
        attempt_number=2,
        provider="fake",
        started_at=started_at,
        completed_at=completed_at,
        failure_code="TIMEOUT",
    )

    assert attempt.status is AttemptStatus.FAILED
    assert attempt.failure_code == "TIMEOUT"
    assert attempt.completed_at == completed_at
