from datetime import datetime, timedelta, timezone

from app.infrastructure.in_memory_worker import (
    InMemoryGenerationJobQueue,
    InMemoryWorkerLeaseRepository,
)


def test_only_one_worker_can_hold_an_active_lease() -> None:
    repo = InMemoryWorkerLeaseRepository()
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)

    first = repo.claim("job-1", "worker-a", now, timedelta(minutes=1))
    second = repo.claim("job-1", "worker-b", now, timedelta(minutes=1))

    assert first is not None
    assert second is None


def test_expired_lease_can_be_recovered_and_reclaimed() -> None:
    repo = InMemoryWorkerLeaseRepository()
    acquired = datetime(2026, 1, 1, tzinfo=timezone.utc)
    expired = acquired + timedelta(minutes=2)

    first = repo.claim("job-1", "worker-a", acquired, timedelta(minutes=1))
    assert first is not None
    assert repo.recover_expired("job-1", expired) is True

    second = repo.claim("job-1", "worker-b", expired, timedelta(minutes=1))
    assert second is not None
    assert second.worker_id == "worker-b"
    assert second.lease_token != first.lease_token


def test_stale_owner_cannot_renew_or_release_recovered_lease() -> None:
    repo = InMemoryWorkerLeaseRepository()
    acquired = datetime(2026, 1, 1, tzinfo=timezone.utc)
    expired = acquired + timedelta(minutes=2)

    first = repo.claim("job-1", "worker-a", acquired, timedelta(minutes=1))
    assert first is not None
    assert repo.recover_expired("job-1", expired) is True

    second = repo.claim("job-1", "worker-b", expired, timedelta(minutes=1))
    assert second is not None

    assert repo.renew("job-1", first.lease_token, expired, timedelta(minutes=1)) is None
    assert repo.release("job-1", first.lease_token) is False
    assert repo.current("job-1") == second


def test_duplicate_delivery_can_be_acked_without_creating_another_logical_job() -> None:
    queue = InMemoryGenerationJobQueue()
    first = queue.enqueue("job-1")
    duplicate = queue.enqueue("job-1")

    assert first.job_id == duplicate.job_id
    assert first.message_id != duplicate.message_id
    assert len({first.job_id, duplicate.job_id}) == 1

    queue.ack(first)
    queue.ack(first)
    assert queue.is_acked(first.message_id) is True


def test_release_requeues_with_incremented_delivery_attempt() -> None:
    queue = InMemoryGenerationJobQueue()
    first = queue.enqueue("job-1")

    queue.release_or_requeue(first)

    pending = queue.pending()
    redeliveries = [m for m in pending if m.job_id == "job-1" and m.message_id != first.message_id]
    assert len(redeliveries) == 1
    assert redeliveries[0].delivery_attempt == 2


def test_acknowledged_message_is_not_requeued() -> None:
    queue = InMemoryGenerationJobQueue()
    first = queue.enqueue("job-1")

    queue.ack(first)
    queue.release_or_requeue(first)

    assert all(message.message_id != first.message_id for message in queue.pending())
