from datetime import datetime, timedelta, timezone

from app.application.ports import GenerationRequest, GenerationResult
from app.application.worker import QueueMessage
from app.application.worker_execution import ExecuteGenerationDelivery, WorkerDeliveryStatus
from app.domain.events import JobEventType
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


class FailOnceAckQueue(InMemoryGenerationJobQueue):
    def __init__(self) -> None:
        super().__init__()
        self.fail_next_ack = True

    def ack(self, message: QueueMessage) -> None:
        if self.fail_next_ack:
            self.fail_next_ack = False
            raise RuntimeError("ack failed")
        super().ack(message)


def test_ack_failure_redelivers_harmlessly_after_durable_completion() -> None:
    queue = FailOnceAckQueue()
    leases = InMemoryWorkerLeaseRepository()
    job = GenerationJob.create("job-1", "video", "scene-1/v1")
    factory, jobs, attempts, events = make_factory(job, leases)
    provider = IdempotentProvider()

    first = queue.enqueue(job.job_id)
    first_result = ExecuteGenerationDelivery(
        queue, leases, factory, Resolver(provider), timedelta(minutes=1)
    ).handle(first, "worker-a", datetime(2026, 1, 1, tzinfo=timezone.utc), {})

    assert first_result.status is WorkerDeliveryStatus.REQUEUED
    assert jobs[job.job_id].status is GenerationStatus.SUCCEEDED
    assert len(provider.requests) == 1

    redelivery = next(m for m in queue.pending() if m.job_id == job.job_id and m.message_id != first.message_id)
    second_result = ExecuteGenerationDelivery(
        queue, leases, factory, Resolver(provider), timedelta(minutes=1)
    ).handle(redelivery, "worker-b", datetime(2026, 1, 1, 0, 1, tzinfo=timezone.utc), {})

    assert second_result.status is WorkerDeliveryStatus.ACKED
    assert len(provider.requests) == 1
    assert len(attempts["job-1:attempt-1"]) == 2
    assert events[-1].event_type.value == "SUCCEEDED"



def test_lease_heartbeat_renews_active_owner_and_preserves_token() -> None:
    queue = InMemoryGenerationJobQueue()
    leases = InMemoryWorkerLeaseRepository()
    job = GenerationJob.create("job-1", "video", "scene-1/v1")
    factory, _, _, _ = make_factory(job, leases)
    provider = IdempotentProvider()
    delivery = ExecuteGenerationDelivery(
        queue, leases, factory, Resolver(provider), timedelta(minutes=1)
    )
    acquired = leases.claim(
        job.job_id,
        "worker-a",
        datetime(2026, 1, 1, tzinfo=timezone.utc),
        timedelta(minutes=1),
    )
    assert acquired is not None

    assert delivery.renew_lease(
        job.job_id,
        acquired.lease_token,
        datetime(2026, 1, 1, 0, 0, 30, tzinfo=timezone.utc),
    ) is True

    current = leases.current(job.job_id)
    assert current is not None
    assert current.lease_token == acquired.lease_token
    assert current.expires_at == datetime(2026, 1, 1, 0, 1, 30, tzinfo=timezone.utc)


def test_lease_heartbeat_rejects_expired_or_stale_owner() -> None:
    queue = InMemoryGenerationJobQueue()
    leases = InMemoryWorkerLeaseRepository()
    job = GenerationJob.create("job-1", "video", "scene-1/v1")
    factory, _, _, _ = make_factory(job, leases)
    provider = IdempotentProvider()
    delivery = ExecuteGenerationDelivery(
        queue, leases, factory, Resolver(provider), timedelta(minutes=1)
    )
    acquired = leases.claim(
        job.job_id,
        "worker-a",
        datetime(2026, 1, 1, tzinfo=timezone.utc),
        timedelta(minutes=1),
    )
    assert acquired is not None

    assert delivery.renew_lease(
        job.job_id,
        acquired.lease_token,
        datetime(2026, 1, 1, 0, 1, tzinfo=timezone.utc),
    ) is False

    replacement = leases.claim(
        job.job_id,
        "worker-b",
        datetime(2026, 1, 1, 0, 1, tzinfo=timezone.utc),
        timedelta(minutes=1),
    )
    assert replacement is not None
    assert delivery.renew_lease(
        job.job_id,
        acquired.lease_token,
        datetime(2026, 1, 1, 0, 1, 1, tzinfo=timezone.utc),
    ) is False


def test_provider_runtime_heartbeat_keeps_lease_alive() -> None:
    queue = InMemoryGenerationJobQueue()
    leases = InMemoryWorkerLeaseRepository()
    job = GenerationJob.create("job-heartbeat", "video", "scene-heartbeat/v1")
    factory, jobs, _, _ = make_factory(job, leases)
    provider = IdempotentProvider()
    heartbeat_times: list[datetime] = []

    def provider_executor(provider_obj, request, heartbeat):
        heartbeat_at = datetime(2026, 1, 1, 0, 0, 30, tzinfo=timezone.utc)
        assert heartbeat(heartbeat_at) is True
        heartbeat_times.append(heartbeat_at)
        return provider_obj.generate(request)

    message = queue.enqueue(job.job_id)
    result = ExecuteGenerationDelivery(
        queue,
        leases,
        factory,
        Resolver(provider),
        timedelta(minutes=1),
        provider_executor=provider_executor,
    ).handle(
        message,
        "worker-a",
        datetime(2026, 1, 1, tzinfo=timezone.utc),
        {},
    )

    assert result.status is WorkerDeliveryStatus.ACKED
    assert heartbeat_times == [datetime(2026, 1, 1, 0, 0, 30, tzinfo=timezone.utc)]
    assert jobs[job.job_id].status is GenerationStatus.SUCCEEDED


def test_failed_provider_runtime_heartbeat_requeues_without_finalizing() -> None:
    queue = InMemoryGenerationJobQueue()
    leases = InMemoryWorkerLeaseRepository()
    job = GenerationJob.create("job-heartbeat-fail", "video", "scene-heartbeat-fail/v1")
    factory, jobs, _, events = make_factory(job, leases)
    provider = IdempotentProvider()
    provider_called = False

    def provider_executor(provider_obj, request, heartbeat):
        nonlocal provider_called
        provider_called = True
        assert heartbeat(datetime(2026, 1, 1, 0, 1, tzinfo=timezone.utc)) is False
        from app.application.execution import LeaseOwnershipLost
        raise LeaseOwnershipLost("heartbeat lost ownership")

    message = queue.enqueue(job.job_id)
    result = ExecuteGenerationDelivery(
        queue,
        leases,
        factory,
        Resolver(provider),
        timedelta(minutes=1),
        provider_executor=provider_executor,
    ).handle(
        message,
        "worker-a",
        datetime(2026, 1, 1, tzinfo=timezone.utc),
        {},
    )

    assert result.status is WorkerDeliveryStatus.REQUEUED
    assert provider_called is True
    assert provider.requests == []
    assert jobs[job.job_id].status is GenerationStatus.QUEUED
    assert events == []
    assert queue.is_acked(message.message_id) is False


class AmbiguousProvider:
    provider_name = "ambiguous-video"

    def __init__(self, contract, reconciliation=None):
        self.execution_contract = contract
        self.reconciliation = reconciliation
        self.requests = []

    def generate(self, request):
        from app.application.reconciliation import AmbiguousProviderOutcome
        self.requests.append(request)
        raise AmbiguousProviderOutcome(
            provider=self.provider_name,
            contract=self.execution_contract,
            reconciliation=self.reconciliation,
        )


class RecoveryResolver:
    def __init__(self, provider):
        self.provider = provider

    def resolve(self, capability):
        return self.provider


class FoundReconciliation:
    def __init__(self, result):
        self.result = result
        self.queries = []

    def reconcile(self, query):
        self.queries.append(query)
        return self.result


def test_ambiguous_idempotent_outcome_requeues_with_stable_key():
    from app.application.reconciliation import ProviderExecutionContract, ProviderOperationSafety

    queue = InMemoryGenerationJobQueue()
    leases = InMemoryWorkerLeaseRepository()
    job = GenerationJob.create("job-ambiguous-idem", "video", "scene-ambiguous/v1")
    factory, jobs, _, _ = make_factory(job, leases)
    provider = AmbiguousProvider(
        ProviderExecutionContract("ambiguous-video", ProviderOperationSafety.IDEMPOTENT)
    )

    message = queue.enqueue(job.job_id)
    result = ExecuteGenerationDelivery(
        queue, leases, factory, RecoveryResolver(provider), timedelta(minutes=1)
    ).handle(message, "worker-a", datetime(2026, 1, 1, tzinfo=timezone.utc), {})

    assert result.status is WorkerDeliveryStatus.REQUEUED
    assert jobs[job.job_id].status is GenerationStatus.QUEUED
    assert provider.requests[0].idempotency_key == "scene-ambiguous/v1"


def test_reconcilable_found_completes_without_second_provider_call():
    from app.application.reconciliation import ProviderExecutionContract, ProviderOperationSafety

    queue = InMemoryGenerationJobQueue()
    leases = InMemoryWorkerLeaseRepository()
    job = GenerationJob.create("job-ambiguous-found", "video", "scene-found/v1")
    factory, jobs, attempts, events = make_factory(job, leases)
    generation = GenerationResult("ambiguous-video", "op-existing", ("asset-existing",))
    reconciliation = FoundReconciliation(generation)
    provider = AmbiguousProvider(
        ProviderExecutionContract("ambiguous-video", ProviderOperationSafety.RECONCILABLE),
        reconciliation,
    )

    message = queue.enqueue(job.job_id)
    result = ExecuteGenerationDelivery(
        queue, leases, factory, RecoveryResolver(provider), timedelta(minutes=1)
    ).handle(message, "worker-a", datetime(2026, 1, 1, tzinfo=timezone.utc), {})

    assert result.status is WorkerDeliveryStatus.ACKED
    assert jobs[job.job_id].status is GenerationStatus.SUCCEEDED
    assert len(provider.requests) == 1
    assert reconciliation.queries[0].idempotency_key == "scene-found/v1"
    assert attempts["job-ambiguous-found:attempt-1"][-1].provider_operation_id == "op-existing"
    assert events[-2].event_type is JobEventType.RECONCILED


def test_reconcilable_not_found_requeues_without_blind_retry():
    from app.application.reconciliation import ProviderExecutionContract, ProviderOperationSafety

    queue = InMemoryGenerationJobQueue()
    leases = InMemoryWorkerLeaseRepository()
    job = GenerationJob.create("job-ambiguous-missing", "video", "scene-missing/v1")
    factory, jobs, _, _ = make_factory(job, leases)

    class MissingReconciliation:
        def reconcile(self, query):
            assert query.idempotency_key == "scene-missing/v1"
            return None

    provider = AmbiguousProvider(
        ProviderExecutionContract("ambiguous-video", ProviderOperationSafety.RECONCILABLE),
        MissingReconciliation(),
    )
    message = queue.enqueue(job.job_id)
    result = ExecuteGenerationDelivery(
        queue, leases, factory, RecoveryResolver(provider), timedelta(minutes=1)
    ).handle(message, "worker-a", datetime(2026, 1, 1, tzinfo=timezone.utc), {})

    assert result.status is WorkerDeliveryStatus.REQUEUED
    assert jobs[job.job_id].status is GenerationStatus.QUEUED


def test_reconciliation_lookup_error_requeues_without_blind_retry():
    from app.application.reconciliation import ProviderExecutionContract, ProviderOperationSafety

    queue = InMemoryGenerationJobQueue()
    leases = InMemoryWorkerLeaseRepository()
    job = GenerationJob.create("job-ambiguous-error", "video", "scene-error/v1")
    factory, jobs, _, _ = make_factory(job, leases)

    class ErrorReconciliation:
        def reconcile(self, query):
            raise RuntimeError("provider lookup unavailable")

    provider = AmbiguousProvider(
        ProviderExecutionContract("ambiguous-video", ProviderOperationSafety.RECONCILABLE),
        ErrorReconciliation(),
    )
    message = queue.enqueue(job.job_id)
    result = ExecuteGenerationDelivery(
        queue, leases, factory, RecoveryResolver(provider), timedelta(minutes=1)
    ).handle(message, "worker-a", datetime(2026, 1, 1, tzinfo=timezone.utc), {})

    assert result.status is WorkerDeliveryStatus.REQUEUED
    assert jobs[job.job_id].status is GenerationStatus.QUEUED


def test_non_reconcilable_ambiguous_outcome_becomes_reconciliation_required():
    from app.application.reconciliation import ProviderExecutionContract, ProviderOperationSafety

    queue = InMemoryGenerationJobQueue()
    leases = InMemoryWorkerLeaseRepository()
    job = GenerationJob.create("job-ambiguous-terminal", "video", "scene-terminal/v1")
    factory, jobs, attempts, events = make_factory(job, leases)
    provider = AmbiguousProvider(
        ProviderExecutionContract("ambiguous-video", ProviderOperationSafety.NON_RECONCILABLE)
    )

    message = queue.enqueue(job.job_id)
    result = ExecuteGenerationDelivery(
        queue, leases, factory, RecoveryResolver(provider), timedelta(minutes=1)
    ).handle(message, "worker-a", datetime(2026, 1, 1, tzinfo=timezone.utc), {})

    assert result.status is WorkerDeliveryStatus.ACKED
    assert jobs[job.job_id].status is GenerationStatus.FAILED
    assert attempts["job-ambiguous-terminal:attempt-1"][-1].failure_code == "RECONCILIATION_REQUIRED"
    assert events[-1].failure_code == "RECONCILIATION_REQUIRED"



def test_reconciled_result_persistence_failure_requeues_without_ack():
    from app.application.reconciliation import ProviderExecutionContract, ProviderOperationSafety

    queue = InMemoryGenerationJobQueue()
    leases = InMemoryWorkerLeaseRepository()
    job = GenerationJob.create("job-ambiguous-persist-fail", "video", "scene-persist-fail/v1")
    factory, jobs, _, _ = make_factory(job, leases, fail_commit=True)
    generation = GenerationResult("ambiguous-video", "op-existing", ("asset-existing",))
    reconciliation = FoundReconciliation(generation)
    provider = AmbiguousProvider(
        ProviderExecutionContract("ambiguous-video", ProviderOperationSafety.RECONCILABLE),
        reconciliation,
    )

    message = queue.enqueue(job.job_id)
    result = ExecuteGenerationDelivery(
        queue, leases, factory, RecoveryResolver(provider), timedelta(minutes=1)
    ).handle(message, "worker-a", datetime(2026, 1, 1, tzinfo=timezone.utc), {})

    assert result.status is WorkerDeliveryStatus.REQUEUED
    assert queue.is_acked(message.message_id) is False
    assert jobs[job.job_id].status is GenerationStatus.QUEUED
    assert len(provider.requests) == 1
    assert reconciliation.queries[0].idempotency_key == "scene-persist-fail/v1"


def test_non_reconcilable_persistence_failure_requeues_without_ack():
    from app.application.reconciliation import ProviderExecutionContract, ProviderOperationSafety

    queue = InMemoryGenerationJobQueue()
    leases = InMemoryWorkerLeaseRepository()
    job = GenerationJob.create("job-ambiguous-terminal-fail", "video", "scene-terminal-fail/v1")
    factory, jobs, _, _ = make_factory(job, leases, fail_commit=True)
    provider = AmbiguousProvider(
        ProviderExecutionContract("ambiguous-video", ProviderOperationSafety.NON_RECONCILABLE)
    )

    message = queue.enqueue(job.job_id)
    result = ExecuteGenerationDelivery(
        queue, leases, factory, RecoveryResolver(provider), timedelta(minutes=1)
    ).handle(message, "worker-a", datetime(2026, 1, 1, tzinfo=timezone.utc), {})

    assert result.status is WorkerDeliveryStatus.REQUEUED
    assert queue.is_acked(message.message_id) is False
    assert jobs[job.job_id].status is GenerationStatus.QUEUED
