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
