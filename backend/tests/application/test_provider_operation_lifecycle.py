from app.application.ports import GenerationRequest, GenerationResult
from app.application.provider_operation import (
    ProviderCancellationResult,
    ProviderCancellationStatus,
    ProviderOperation,
    ProviderOperationStatus,
    ProviderOperationStatusResult,
)
from app.application.provider_operation_lifecycle import ProviderOperationLifecycle
from app.application.reconciliation import (\n    AmbiguousProviderOutcome,\n    ProviderExecutionContract,\n    ProviderOperationSafety,\n)
from app.infrastructure.in_memory_persistence import InMemoryPersistenceTransaction


class FakeProvider:
    provider_name = "test-provider"

    def __init__(self):
        self.submissions = 0
        self.polls = 0
        self.cancellations = 0
        self.status = ProviderOperationStatus.SUBMITTED

    def submit(self, request):
        self.submissions += 1
        return ProviderOperation(
            provider=self.provider_name,
            operation_id=f"op-{self.submissions}",
            idempotency_key=request.idempotency_key,
            capability=request.capability,
        )

    def get_status(self, operation):
        self.polls += 1
        return ProviderOperationStatusResult(
            operation=operation,
            status=self.status,
            result=(
                GenerationResult(
                    provider=self.provider_name,
                    provider_operation_id=operation.operation_id,
                    artifact_refs=("artifact://video/1",),
                )
                if self.status is ProviderOperationStatus.SUCCEEDED
                else None
            ),
        )

    def cancel(self, operation):
        self.cancellations += 1
        return ProviderCancellationResult(status=ProviderCancellationStatus.ACCEPTED)


def request():
    return GenerationRequest(
        job_id="job-1",
        capability="video",
        inputs={"prompt": "test"},
        idempotency_key="job-1",
    )


def lifecycle(transaction, provider=None):
    return ProviderOperationLifecycle(
        transaction=transaction,
        provider=provider or FakeProvider(),
        provider_name="test-provider",
        contract=ProviderExecutionContract(
            provider="test-provider",
            operation_safety=ProviderOperationSafety.IDEMPOTENT,
        ),
    )


def test_submit_persists_operation_before_returning_identity():
    tx = InMemoryPersistenceTransaction()
    provider = FakeProvider()
    service = lifecycle(tx, provider)

    operation = service.submit(request())

    assert operation.operation_id == "op-1"
    assert tx.provider_operations.get("test-provider", "op-1") == operation
    assert tx.commit_count == 1


def test_submission_persistence_failure_surfaces_ambiguous_outcome_and_does_not_leave_local_state():
    tx = InMemoryPersistenceTransaction()
    tx.fail_commit = True
    provider = FakeProvider()
    service = lifecycle(tx, provider)

    try:
        service.submit(request())
    except AmbiguousProviderOutcome:
        pass
    else:
        raise AssertionError("submission persistence failure must remain ambiguous")

    assert provider.submissions == 1
    assert tx.provider_operations.get("test-provider", "op-1") is None


def test_duplicate_delivery_reuses_durable_operation_without_resubmission():
    tx = InMemoryPersistenceTransaction()
    provider = FakeProvider()
    service = lifecycle(tx, provider)

    first = service.submit(request())
    second = service.submit(request())

    assert second == first
    assert provider.submissions == 1


def test_poll_persists_running_status_and_preserves_identity():
    tx = InMemoryPersistenceTransaction()
    provider = FakeProvider()
    service = lifecycle(tx, provider)
    operation = service.submit(request())
    provider.status = ProviderOperationStatus.RUNNING

    observed = service.poll(operation)
    stored = tx.provider_operations.get("test-provider", "op-1")

    assert observed.status is ProviderOperationStatus.RUNNING
    assert stored is not None
    assert stored.status is ProviderOperationStatus.RUNNING
    assert stored.operation_id == operation.operation_id


def test_poll_terminal_success_returns_normalized_generation_result():
    tx = InMemoryPersistenceTransaction()
    provider = FakeProvider()
    service = lifecycle(tx, provider)
    operation = service.submit(request())
    provider.status = ProviderOperationStatus.SUCCEEDED

    observed = service.poll(operation)

    assert observed.status is ProviderOperationStatus.SUCCEEDED
    assert isinstance(observed.result, GenerationResult)
    assert observed.result.provider_operation_id == operation.operation_id


def test_repeated_poll_is_harmless_and_durable():
    tx = InMemoryPersistenceTransaction()
    provider = FakeProvider()
    service = lifecycle(tx, provider)
    operation = service.submit(request())
    provider.status = ProviderOperationStatus.RUNNING

    service.poll(operation)
    service.poll(operation)

    stored = tx.provider_operations.get("test-provider", operation.operation_id)
    assert provider.polls == 2
    assert stored is not None
    assert stored.status is ProviderOperationStatus.RUNNING


def test_cancel_does_not_claim_terminal_completion_when_provider_only_accepts_request():
    tx = InMemoryPersistenceTransaction()
    provider = FakeProvider()
    service = lifecycle(tx, provider)
    operation = service.submit(request())

    result = service.cancel(operation)

    assert result.status is ProviderCancellationStatus.ACCEPTED
    assert provider.cancellations == 1
    stored = tx.provider_operations.get("test-provider", operation.operation_id)
    assert stored is not None
    assert stored.status is ProviderOperationStatus.SUBMITTED


def test_cancel_skips_external_call_for_already_terminal_operation():
    tx = InMemoryPersistenceTransaction()
    provider = FakeProvider()
    service = lifecycle(tx, provider)
    operation = service.submit(request())
    provider.status = ProviderOperationStatus.SUCCEEDED
    service.poll(operation)

    result = service.cancel(operation)

    assert result.status is ProviderCancellationStatus.ALREADY_TERMINAL
    assert provider.cancellations == 0
