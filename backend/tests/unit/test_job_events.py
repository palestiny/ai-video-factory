from dataclasses import FrozenInstanceError
from datetime import datetime, timezone

import pytest

from app.domain.events import JobEvent, JobEventType


def test_job_event_is_immutable_and_carries_execution_context():
    event = JobEvent(
        event_id="event-1",
        job_id="job-1",
        event_type=JobEventType.FAILED,
        occurred_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        attempt_number=2,
        failure_code="TIMEOUT",
        metadata=(("provider", "fake"),),
    )

    assert event.event_type is JobEventType.FAILED
    assert event.attempt_number == 2
    assert event.metadata == (("provider", "fake"),)

    with pytest.raises(FrozenInstanceError):
        event.event_type = JobEventType.SUCCEEDED


def test_job_event_rejects_invalid_identity_and_attempt():
    occurred_at = datetime(2026, 1, 1, tzinfo=timezone.utc)

    with pytest.raises(ValueError):
        JobEvent(
            event_id=" ",
            job_id="job-1",
            event_type=JobEventType.CREATED,
            occurred_at=occurred_at,
        )

    with pytest.raises(ValueError):
        JobEvent(
            event_id="event-1",
            job_id="job-1",
            event_type=JobEventType.STARTED,
            occurred_at=occurred_at,
            attempt_number=0,
        )
