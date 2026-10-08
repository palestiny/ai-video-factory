from datetime import datetime, timedelta, timezone

from app.application.ports import GenerationRequest, GenerationResult
from app.application.provider_operation import (
    ProviderCancellationResult,
    ProviderOperation,
    ProviderOperationStatus,
    ProviderOperationStatusResult,
)
from app.application.reconciliation import ProviderExecutionContract, ProviderOperationSafety
from app.application.async_worker_execution import ExecuteAsyncProviderDelivery
from app.application.worker_execution import WorkerDeliveryStatus
from app.domain.generation import GenerationJob, GenerationStatus
from app.infrastructure.in_memory_persistence import InMemoryPersistenceTransaction
from app.infrastructure.in_memory_worker import InMemoryGenerationJobQueue, InMemoryWorkerLeaseRepository


NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


class AsyncFakeProvider:
    provider_name = "async-fake"

    def __init__(self):
        self.submissions = 0
        self.polls = 0
        self.status = ProviderOperationStatus.SUBMITTED

    def submit(self, request: GenerationRequest) -> ProviderOperation:
        self.submissions += 1
        return ProviderOperation(
            provider=self.provider_name,
            operation_id=f"op-{self.submissions}",
            idempotency_key=request.idempotency_key,
            capability=request.capability,
        )

    def get_status(self, operation: ProviderOperation) -> ProviderOperationStatusResult:
        self.polls += 1
        result = (
            GenerationResult(self.provider_name, operation.operation_id, ("asset://final.mp4",))
            if self.status is ProviderOperationStatus.SUCCEEDED
            else None
        )
        return ProviderOperationStatusResult(
            operation=operation,
            status=self.status,
            result=result,
            failure_code="PROVIDER_FAILURE" if self.status is ProviderOperationStatus.FAILED else None,
        )

    def cancel(self, operation: ProviderOperation) -> ProviderCancellationResult:
        raise AssertionError("cancel is not expected in this delivery test")


class AsyncResolver:
    def __init__(self, provider):
        self.provider = provider

    def resolve(self, capability: str):
        assert capability == "video"
        return self.provider


class ContractResolver:
    def resolve_contract(self, provider_name: str):
        assert provider_name == "async-fake"
        return ProviderExecutionContract(provider_name, ProviderOperationSafety.IDEMPOTENT)


def build(*, fail_finalize_once: bool = False):
    queue = InMemoryGenerationJobQueue()
    leases = InMemoryWorkerLeaseRepository()
    job = GenerationJob.create("async-job", "video", "async-job/v1")
    jobs = {job.job_id: job}
    attempts = {}
    events = []
    operation_store = {}
    created_transactions = 0

    def factory():
        nonlocal created_transactions
        created_transactions += 1
        tx = InMemoryPersistenceTransaction(
            jobs=jobs,
            attempts=attempts,
            provider_operations=operation_store,
            events=events,
            lease_repository=leases,
        )
        # The first delivery uses one transaction for start/submit/poll. Its
        # second transaction is terminal job finalization.
        tx.fail_commit = fail_finalize_once and created_transactions == 2
        return tx

    provider = AsyncFakeProvider()
    worker = ExecuteAsyncProviderDelivery(
        queue, leases, factory, AsyncResolver(provider), ContractResolver(),
        timedelta(minutes=1), poll_delay=timedelta(seconds=3),
    )
    return queue, leases, job, jobs, attempts, events, provider, worker


def test_nonterminal_operation_schedules_next_delivery_and_releases_lease():
    queue, leases, job, jobs, attempts, events, provider, worker = build()
    message = queue.enqueue(job.job_id)

    result = worker.handle(message, "worker-a", NOW, {"prompt": "a shot"})

    assert result.status is WorkerDeliveryStatus.ACKED
    assert jobs[job.job_id].status is GenerationStatus.RUNNING
    assert provider.submissions == 1
    assert provider.polls == 1
    assert len(attempts["async-job:attempt-1"]) == 1
    assert queue.is_acked(message.message_id)
    assert leases.current(job.job_id) is None
    assert len(queue.pending()) == 1


def test_terminal_success_completes_job_and_attempt_on_later_delivery():
    queue, leases, job, jobs, attempts, events, provider, worker = build()
    first = queue.enqueue(job.job_id)
    worker.handle(first, "worker-a", NOW, {"prompt": "a shot"})
    next_message = queue.pending()[0]
    provider.status = ProviderOperationStatus.SUCCEEDED

    result = worker.handle(next_message, "worker-b", NOW + timedelta(seconds=4), {})

    assert result.status is WorkerDeliveryStatus.ACKED
    assert jobs[job.job_id].status is GenerationStatus.SUCCEEDED
    assert provider.submissions == 1
    assert attempts["async-job:attempt-1"][-1].status.value == "SUCCEEDED"
    assert attempts["async-job:attempt-1"][-1].provider_operation_id == "op-1"
    assert events[-1].event_type.value == "SUCCEEDED"


def test_terminal_operation_replay_finishes_job_after_commit_failure_without_resubmit():
    queue, leases, job, jobs, attempts, events, provider, worker = build(fail_finalize_once=True)
    message = queue.enqueue(job.job_id)
    provider.status = ProviderOperationStatus.SUCCEEDED

    first = worker.handle(message, "worker-a", NOW, {"prompt": "a shot"})

    assert first.status is WorkerDeliveryStatus.REQUEUED
    assert jobs[job.job_id].status is GenerationStatus.RUNNING
    assert provider.submissions == 1
    assert provider.polls == 1

    redelivery = next(item for item in queue.pending() if item.message_id != message.message_id)
    second = worker.handle(redelivery, "worker-b", NOW + timedelta(seconds=1), {})

    assert second.status is WorkerDeliveryStatus.ACKED
    assert jobs[job.job_id].status is GenerationStatus.SUCCEEDED
    assert provider.submissions == 1
    assert provider.polls == 1
