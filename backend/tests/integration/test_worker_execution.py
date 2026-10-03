from datetime import datetime, timedelta, timezone

from app.application.ports import GenerationRequest, GenerationResult
from app.application.worker import QueueMessage
from app.application.worker_execution import ExecuteGenerationDelivery, WorkerDeliveryStatus
from app.domain.generation import GenerationJob, GenerationStatus
from app.infrastructure.in_memory_persistence import InMemoryPersistenceTransaction
from app.infrastructure.in_memory_worker import InMemoryGenerationJobQueue, InMemoryWorkerLeaseRepository


class IdempotentProvider:
    provider_name = "fake-video"

    def __init__(self) -> None:
        self.requests: list[GenerationRequest] = []
        self._results: dict[str, GenerationResult] = {}

    def generate(self, request: GenerationRequest) -> GenerationResult:
        self.requests.append(request)
        if request.idempotency_key not in self._results:
            self._results[request.idempotency_key] = GenerationResult(
                provider=self.provider_name,
                provider_operation_id=f"op-{len(self._results) + 1}",
                artifact_refs=("asset-1",),
            )
        return self._results[request.idempotency_key]


class Resolver:
    def __init__(self, provider: IdempotentProvider) -> None:
        self.provider = provider

    def resolve(self, capability: str) -> IdempotentProvider:
        assert capability == "video"
        return self.provider


def make_factory(
    job: GenerationJob,
    leases: InMemoryWorkerLeaseRepository,
    *,
    fail_commit: bool = False,
):
    jobs = {job.job_id: job}
    attempts = {}
    events = []
    provider_store = {}

    def factory() -> InMemoryPersistenceTransaction:
        tx = InMemoryPersistenceTransaction(
            jobs=jobs,
            attempts=attempts,
            events=events,
            lease_repository=leases,
        )
        tx.fail_commit = fail_commit
        return tx

    return factory, jobs, attempts, events


def test_cancelled_job_is_acked_without_provider_execution() -> None:
    queue = InMemoryGenerationJobQueue()
    leases = InMemoryWorkerLeaseRepository()
    job = GenerationJob.create("job-1", "video", "scene-1/v1")
    job.cancel()
    factory, jobs, _, _ = make_factory(job, leases)
    provider = IdempotentProvider()

    message = queue.enqueue(job.job_id)
    result = ExecuteGenerationDelivery(
        queue, leases, factory, Resolver(provider), timedelta(minutes=1)
    ).handle(message, "worker-a", datetime(2026, 1, 1, tzinfo=timezone.utc), {})

    assert result.status is WorkerDeliveryStatus.ACKED
    assert provider.requests == []
    assert queue.is_acked(message.message_id) is True
    assert leases.current(job.job_id) is None


def test_success_is_durably_persisted_before_ack() -> None:
    queue = InMemoryGenerationJobQueue()
    leases = InMemoryWorkerLeaseRepository()
    job = GenerationJob.create("job-1", "video", "scene-1/v1")
    factory, jobs, attempts, events = make_factory(job, leases)
    provider = IdempotentProvider()

    message = queue.enqueue(job.job_id)
    result = ExecuteGenerationDelivery(
        queue, leases, factory, Resolver(provider), timedelta(minutes=1)
    ).handle(message, "worker-a", datetime(2026, 1, 1, tzinfo=timezone.utc), {"prompt": "shot"})

    assert result.status is WorkerDeliveryStatus.ACKED
    assert jobs["job-1"].status is GenerationStatus.SUCCEEDED
    assert len(attempts["job-1:attempt-1"]) == 2
    assert events[-1].event_type.value == "SUCCEEDED"
    assert queue.is_acked(message.message_id) is True


def test_commit_failure_requeues_delivery_and_stable_key_is_reused() -> None:
    queue = InMemoryGenerationJobQueue()
    leases = InMemoryWorkerLeaseRepository()
    job = GenerationJob.create("job-1", "video", "scene-1/v1")
    provider = IdempotentProvider()
    factory, jobs, _, _ = make_factory(job, leases, fail_commit=True)

    first = queue.enqueue(job.job_id)
    first_result = ExecuteGenerationDelivery(
        queue, leases, factory, Resolver(provider), timedelta(minutes=1)
    ).handle(first, "worker-a", datetime(2026, 1, 1, tzinfo=timezone.utc), {})

    assert first_result.status is WorkerDeliveryStatus.REQUEUED
    assert queue.is_acked(first.message_id) is False
    redelivery = next(m for m in queue.pending() if m.job_id == job.job_id and m.message_id != first.message_id)

    # The provider sees the same stable idempotency key on redelivery.
    factory_ok, jobs, attempts, events = make_factory(job, leases, fail_commit=False)
    second_result = ExecuteGenerationDelivery(
        queue, leases, factory_ok, Resolver(provider), timedelta(minutes=1)
    ).handle(redelivery, "worker-b", datetime(2026, 1, 1, tzinfo=timezone.utc), {})

    assert second_result.status is WorkerDeliveryStatus.ACKED
    assert [request.idempotency_key for request in provider.requests] == ["scene-1/v1", "scene-1/v1"]
    assert provider.requests[0].idempotency_key == provider.requests[1].idempotency_key
    assert jobs["job-1"].status is GenerationStatus.SUCCEEDED
    assert queue.is_acked(redelivery.message_id) is True


def test_stale_worker_cannot_finalize_after_lease_recovery() -> None:
    queue = InMemoryGenerationJobQueue()
    leases = InMemoryWorkerLeaseRepository()
    job = GenerationJob.create("job-1", "video", "scene-1/v1")
    factory, jobs, _, events = make_factory(job, leases)
    provider = IdempotentProvider()

    message = queue.enqueue(job.job_id)
    first_lease = leases.claim(
        job.job_id, "worker-a", datetime(2026, 1, 1, tzinfo=timezone.utc), timedelta(minutes=1)
    )
    assert first_lease is not None
    assert leases.recover_expired(job.job_id, datetime(2026, 1, 2, tzinfo=timezone.utc)) is True
    second_lease = leases.claim(
        job.job_id, "worker-b", datetime(2026, 1, 2, tzinfo=timezone.utc), timedelta(minutes=1)
    )
    assert second_lease is not None

    # A transaction carrying the stale token must be rejected at terminal persistence.
    tx = InMemoryPersistenceTransaction(
        jobs=jobs, attempts={}, events=events, lease_repository=leases
    )
    from app.application.execution import ExecuteGenerationJob, LeaseOwnershipLost, ExecuteGenerationCommand

    try:
        ExecuteGenerationJob(tx, Resolver(provider)).execute(
            ExecuteGenerationCommand(job.job_id, {}, lease_token=first_lease.lease_token)
        )
    except LeaseOwnershipLost:
        pass
    else:
        raise AssertionError("stale worker was allowed to finalize")

    assert jobs[job.job_id].status is GenerationStatus.QUEUED
    assert second_lease.lease_token == leases.current(job.job_id).lease_token


def test_running_job_is_recovered_after_expired_lease() -> None:
    queue = InMemoryGenerationJobQueue()
    leases = InMemoryWorkerLeaseRepository()
    job = GenerationJob.create("job-1", "video", "scene-1/v1")
    job.start()
    jobs = {job.job_id: job}
    attempts = {}
    events = []
    provider = IdempotentProvider()

    def factory():
        return InMemoryPersistenceTransaction(
            jobs=jobs, attempts=attempts, events=events, lease_repository=leases
        )

    message = queue.enqueue(job.job_id)
    result = ExecuteGenerationDelivery(
        queue, leases, factory, Resolver(provider), timedelta(minutes=1)
    ).handle(message, "worker-a", datetime(2026, 1, 2, tzinfo=timezone.utc), {})

    assert result.status is WorkerDeliveryStatus.ACKED
    assert jobs[job.job_id].status is GenerationStatus.SUCCEEDED
    assert [event.event_type.value for event in events][:2] == ["RECOVERED", "STARTED"]
    assert jobs[job.job_id].attempt_count == 2
