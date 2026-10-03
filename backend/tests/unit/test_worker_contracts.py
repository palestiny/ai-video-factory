from datetime import datetime, timedelta, timezone

import pytest

from app.application.worker import QueueMessage, WorkerLease


def test_queue_message_requires_identity_and_positive_delivery_attempt() -> None:
    message = QueueMessage(message_id="m1", job_id="job-1")
    assert message.delivery_attempt == 1

    with pytest.raises(ValueError, match="message_id"):
        QueueMessage(message_id=" ", job_id="job-1")

    with pytest.raises(ValueError, match="delivery_attempt"):
        QueueMessage(message_id="m1", job_id="job-1", delivery_attempt=0)


def test_worker_lease_requires_non_expired_interval() -> None:
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    lease = WorkerLease(
        job_id="job-1",
        worker_id="worker-1",
        lease_token="lease-1",
        acquired_at=now,
        expires_at=now + timedelta(minutes=1),
    )

    assert lease.expires_at > lease.acquired_at

    with pytest.raises(ValueError, match="expires_at"):
        WorkerLease(
            job_id="job-1",
            worker_id="worker-1",
            lease_token="lease-1",
            acquired_at=now,
            expires_at=now,
        )


def test_worker_lease_rejects_blank_identity() -> None:
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    expires = now + timedelta(minutes=1)

    with pytest.raises(ValueError, match="job_id"):
        WorkerLease("", "worker-1", "lease-1", now, expires)

    with pytest.raises(ValueError, match="worker_id"):
        WorkerLease("job-1", "", "lease-1", now, expires)

    with pytest.raises(ValueError, match="lease_token"):
        WorkerLease("job-1", "worker-1", "", now, expires)
