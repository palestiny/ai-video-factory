from __future__ import annotations

from copy import deepcopy

from app.application.provider_operation import ProviderOperation
from app.application.provider_operation_repository import ProviderOperationRepository


class InMemoryProviderOperationRepository(ProviderOperationRepository):
    """Deterministic repository double keyed by provider operation identity."""

    def __init__(self, items: dict[tuple[str, str], ProviderOperation] | None = None) -> None:
        self._items = items if items is not None else {}

    def add(self, operation: ProviderOperation) -> None:
        key = (operation.provider, operation.operation_id)
        if key in self._items:
            raise ValueError(f"provider operation already exists: {key}")
        existing = self.get_by_idempotency_key(operation.provider, operation.idempotency_key)
        if existing is not None:
            raise ValueError("provider operation idempotency key already exists")
        self._items[key] = deepcopy(operation)

    def get(self, provider: str, operation_id: str) -> ProviderOperation | None:
        operation = self._items.get((provider, operation_id))
        return deepcopy(operation) if operation is not None else None

    def save(self, operation: ProviderOperation) -> None:
        key = (operation.provider, operation.operation_id)
        if key not in self._items:
            raise KeyError(f"provider operation not found: {key}")
        self._items[key] = deepcopy(operation)

    def get_by_idempotency_key(
        self,
        provider: str,
        idempotency_key: str,
    ) -> ProviderOperation | None:
        for operation in self._items.values():
            if (
                operation.provider == provider
                and operation.idempotency_key == idempotency_key
            ):
                return deepcopy(operation)
        return None
