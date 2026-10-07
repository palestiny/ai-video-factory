from __future__ import annotations

from typing import Protocol

from app.application.provider_operation import ProviderOperation


class ProviderOperationRepository(Protocol):
    """Durable identity/state boundary for external provider operations."""

    def add(self, operation: ProviderOperation) -> None:
        ...

    def get(self, provider: str, operation_id: str) -> ProviderOperation | None:
        ...

    def save(self, operation: ProviderOperation) -> None:
        ...

    def get_by_idempotency_key(
        self,
        provider: str,
        idempotency_key: str,
    ) -> ProviderOperation | None:
        ...
