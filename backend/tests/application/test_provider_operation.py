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


from app.infrastructure.in_memory_provider_operation import (
    InMemoryProviderOperationRepository,
)


def operation() -> ProviderOperation:
    return ProviderOperation(
        provider="test-provider",
        operation_id="op-123",
        idempotency_key="job/1",
        capability="video",
    )


def test_operation_survives_repository_round_trip():
    repo = InMemoryProviderOperationRepository()
    repo.add(operation())

    loaded = repo.get("test-provider", "op-123")

    assert loaded == operation()


def test_operation_identity_is_durable_across_status_update():
    repo = InMemoryProviderOperationRepository()
    repo.add(operation())

    updated = ProviderOperation(
        provider="test-provider",
        operation_id="op-123",
        idempotency_key="job/1",
        capability="video",
        status=ProviderOperationStatus.RUNNING,
        version=1,
    )
    repo.save(updated)

    loaded = repo.get("test-provider", "op-123")

    assert loaded is not None
    assert loaded.operation_id == "op-123"
    assert loaded.idempotency_key == "job/1"
    assert loaded.status is ProviderOperationStatus.RUNNING


def test_idempotency_key_resolves_existing_operation_without_resubmission():
    repo = InMemoryProviderOperationRepository()
    repo.add(operation())

    loaded = repo.get_by_idempotency_key("test-provider", "job/1")

    assert loaded is not None
    assert loaded.operation_id == "op-123"


def test_duplicate_operation_identity_is_rejected():
    repo = InMemoryProviderOperationRepository()
    repo.add(operation())

    try:
        repo.add(operation())
    except ValueError as exc:
        assert "already exists" in str(exc)
    else:
        raise AssertionError("duplicate operation must be rejected")



def test_poll_generation_defaults_to_zero_and_rejects_negative_values():
    from dataclasses import replace

    current = operation()
    assert current.poll_generation == 0

    try:
        replace(current, poll_generation=-1)
    except ValueError as exc:
        assert "poll_generation" in str(exc)
    else:
        raise AssertionError("negative poll generation must be rejected")


def test_poll_generation_survives_repository_round_trip():
    from dataclasses import replace

    repo = InMemoryProviderOperationRepository()
    current = replace(operation(), poll_generation=7)
    repo.add(current)

    assert repo.get("test-provider", "op-123") == current
