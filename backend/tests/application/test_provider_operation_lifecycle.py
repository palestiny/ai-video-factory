from app.application.ports import GenerationRequest, GenerationResult
from app.application.provider_operation import (
    ProviderCancellationResult,
    ProviderCancellationStatus,
    ProviderOperation,
    ProviderOperationStatus,
    ProviderOperationStatusResult,
)
from app.application.provider_operation_lifecycle import (
    ProviderOperationLifecycle,
    ProviderOperationPersistenceUncertain,
)
from app.application.reconciliation import (
    AmbiguousProviderOutcome,
    ProviderExecutionContract,
    ProviderOperationSafety,
)
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



def test_poll_persistence_failure_uses_recoverable_uncertainty_signal():
    tx = InMemoryPersistenceTransaction()
    provider = FakeProvider()
    service = lifecycle(tx, provider)
    operation = service.submit(request())
    provider.status = ProviderOperationStatus.RUNNING
    tx.fail_commit = True

    try:
        service.poll(operation)
    except ProviderOperationPersistenceUncertain:
        pass
    else:
        raise AssertionError("poll persistence failure must remain explicitly uncertain")

    stored = tx.provider_operations.get("test-provider", operation.operation_id)
    assert stored is not None
    assert stored.status is ProviderOperationStatus.SUBMITTED



def test_terminal_result_survives_replay_without_polling_or_status_regression():
    tx = InMemoryPersistenceTransaction()
    provider = FakeProvider()
    service = lifecycle(tx, provider)
    operation = service.submit(request())
    provider.status = ProviderOperationStatus.SUCCEEDED

    first = service.poll(operation)
    provider.status = ProviderOperationStatus.RUNNING
    replayed = lifecycle(tx, provider).poll(operation)

    assert first.status is ProviderOperationStatus.SUCCEEDED
    assert replayed.status is ProviderOperationStatus.SUCCEEDED
    assert replayed.result == first.result
    assert provider.polls == 1

    stored = tx.provider_operations.get("test-provider", operation.operation_id)
    assert stored is not None
    assert stored.status is ProviderOperationStatus.SUCCEEDED
    assert stored.terminal_result == first.result


def test_terminal_failure_code_survives_replay():
    tx = InMemoryPersistenceTransaction()
    provider = FakeProvider()
    service = lifecycle(tx, provider)
    operation = service.submit(request())
    provider.status = ProviderOperationStatus.FAILED

    provider.get_status = lambda current: ProviderOperationStatusResult(
        operation=current,
        status=ProviderOperationStatus.FAILED,
        failure_code="PROVIDER_FAILURE",
        diagnostics={"source": "provider"},
    )
    first = service.poll(operation)
    replayed = lifecycle(tx, provider).poll(operation)

    assert replayed.status is ProviderOperationStatus.FAILED
    assert replayed.failure_code == "PROVIDER_FAILURE"
    assert replayed.diagnostics == {"source": "provider"}
    assert replayed.failure_code == first.failure_code


def test_poll_atomically_persists_next_work_intent_and_generation():
    from datetime import datetime, timedelta, timezone

    tx = InMemoryPersistenceTransaction()
    provider = FakeProvider()
    service = lifecycle(tx, provider)
    operation = service.submit(request())
    provider.status = ProviderOperationStatus.RUNNING
    due_at = datetime.now(timezone.utc) + timedelta(seconds=5)

    observed = service.poll(
        operation,
        next_poll_due_at=due_at,
        expected_poll_generation=0,
        job_id="job-1",
    )

    stored = tx.provider_operations.get("test-provider", operation.operation_id)
    from app.application.work_intent import WorkIntent
    intent_key = tx.work_intents.get(
        WorkIntent.provider_poll_key("test-provider", operation.operation_id, 1)
    )
    assert observed.status is ProviderOperationStatus.RUNNING
    assert stored is not None and stored.poll_generation == 1
    assert intent_key is not None
    assert intent_key.due_at == due_at


def test_replayed_old_poll_generation_does_not_poll_or_schedule_again():
    from datetime import datetime, timedelta, timezone

    tx = InMemoryPersistenceTransaction()
    provider = FakeProvider()
    service = lifecycle(tx, provider)
    operation = service.submit(request())
    provider.status = ProviderOperationStatus.RUNNING
    due_at = datetime.now(timezone.utc) + timedelta(seconds=5)

    service.poll(
        operation,
        next_poll_due_at=due_at,
        expected_poll_generation=0,
        job_id="job-1",
    )
    replayed = service.poll(
        operation,
        next_poll_due_at=due_at + timedelta(seconds=5),
        expected_poll_generation=0,
        job_id="job-1",
    )

    stored = tx.provider_operations.get("test-provider", operation.operation_id)
    assert provider.polls == 1
    assert replayed.operation.poll_generation == 1
    assert stored is not None and stored.poll_generation == 1


def test_poll_intent_and_operation_generation_roll_back_together():
    from datetime import datetime, timedelta, timezone
    from app.application.work_intent import WorkIntent

    tx = InMemoryPersistenceTransaction()
    provider = FakeProvider()
    service = lifecycle(tx, provider)
    operation = service.submit(request())
    provider.status = ProviderOperationStatus.RUNNING
    tx.fail_commit = True

    try:
        service.poll(
            operation,
            next_poll_due_at=datetime.now(timezone.utc) + timedelta(seconds=5),
            expected_poll_generation=0,
            job_id="job-1",
        )
    except ProviderOperationPersistenceUncertain:
        pass
    else:
        raise AssertionError("failed commit must surface uncertain persistence")

    stored = tx.provider_operations.get("test-provider", operation.operation_id)
    assert stored is not None
    assert stored.poll_generation == 0
    assert stored.status is ProviderOperationStatus.SUBMITTED
    assert tx.work_intents.get(
        WorkIntent.provider_poll_key("test-provider", operation.operation_id, 1)
    ) is None
