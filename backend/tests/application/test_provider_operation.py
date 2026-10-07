from app.application.provider_operation import (
    ProviderCancellationResult,
    ProviderCancellationStatus,
    ProviderOperation,
    ProviderOperationStatus,
    ProviderOperationStatusResult,
)


def test_provider_operation_preserves_stable_identity():
    operation = ProviderOperation(
        provider="test-provider",
        operation_id="op-123",
        idempotency_key="job/1",
        capability="video",
    )

    assert operation.operation_id == "op-123"
    assert operation.idempotency_key == "job/1"
    assert operation.status is ProviderOperationStatus.SUBMITTED


def test_provider_operation_status_can_represent_async_lifecycle():
    operation = ProviderOperation(
        provider="test-provider",
        operation_id="op-123",
        idempotency_key="job/1",
        capability="video",
    )

    running = ProviderOperationStatusResult(
        operation=operation,
        status=ProviderOperationStatus.RUNNING,
    )
    completed = ProviderOperationStatusResult(
        operation=operation,
        status=ProviderOperationStatus.SUCCEEDED,
    )

    assert running.status is ProviderOperationStatus.RUNNING
    assert completed.status is ProviderOperationStatus.SUCCEEDED


def test_cancellation_unknown_is_explicit():
    result = ProviderCancellationResult(
        status=ProviderCancellationStatus.UNKNOWN,
    )

    assert result.status is ProviderCancellationStatus.UNKNOWN
