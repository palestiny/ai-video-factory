import pytest

from app.application.idempotency import (
    IdempotencyConflict,
    InMemoryIdempotencyRepository,
    ReservationStatus,
    normalize_request_fingerprint,
)


def test_first_reservation_creates_logical_request():
    repository = InMemoryIdempotencyRepository()

    result = repository.reserve(
        key=" Scene-1 / V1 ",
        request_fingerprint="video:scene-1:v1",
        job_id="job-1",
    )

    assert result.status is ReservationStatus.CREATED
    assert result.key == "scene-1/v1"
    assert result.job_id == "job-1"


def test_repeated_equivalent_reservation_returns_existing_job():
    repository = InMemoryIdempotencyRepository()

    first = repository.reserve(
        key="scene-1/v1",
        request_fingerprint="video:scene-1:v1",
        job_id="job-1",
    )
    second = repository.reserve(
        key=" SCENE-1 / V1 ",
        request_fingerprint=" video:scene-1:v1 ",
        job_id="job-2",
    )

    assert first.status is ReservationStatus.CREATED
    assert second.status is ReservationStatus.EXISTING
    assert second.job_id == "job-1"


def test_same_key_with_different_request_is_conflict():
    repository = InMemoryIdempotencyRepository()
    repository.reserve(
        key="scene-1/v1",
        request_fingerprint="video:scene-1:v1",
        job_id="job-1",
    )

    with pytest.raises(IdempotencyConflict):
        repository.reserve(
            key="scene-1/v1",
            request_fingerprint="video:scene-1:v2",
            job_id="job-2",
        )


def test_blank_fingerprint_is_rejected():
    with pytest.raises(ValueError):
        normalize_request_fingerprint("   ")


def test_blank_job_id_is_rejected():
    repository = InMemoryIdempotencyRepository()

    with pytest.raises(ValueError):
        repository.reserve(
            key="scene-1/v1",
            request_fingerprint="video:scene-1:v1",
            job_id="   ",
        )
