from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Protocol

from app.application.ports import GenerationResult


class ProviderOperationSafety(str, Enum):
    """How an adapter makes a repeated logical operation safe to recover."""

    IDEMPOTENT = "IDEMPOTENT"
    RECONCILABLE = "RECONCILABLE"
    NON_RECONCILABLE = "NON_RECONCILABLE"


@dataclass(frozen=True)
class ProviderExecutionContract:
    """Explicit provider recovery guarantees for one capability adapter."""

    provider: str
    operation_safety: ProviderOperationSafety

    def __post_init__(self) -> None:
        if not self.provider.strip():
            raise ValueError("provider cannot be blank")

    @property
    def can_recover_ambiguity(self) -> bool:
        return self.operation_safety in {
            ProviderOperationSafety.IDEMPOTENT,
            ProviderOperationSafety.RECONCILABLE,
        }


@dataclass(frozen=True)
class ReconciliationQuery:
    """Stable identity used to locate a possibly-created external operation."""

    idempotency_key: str

    def __post_init__(self) -> None:
        if not self.idempotency_key.strip():
            raise ValueError("idempotency_key cannot be blank")


class GenerationReconciliationPort(Protocol):
    """Optional adapter boundary for providers without native idempotency."""

    def reconcile(self, query: ReconciliationQuery) -> GenerationResult | None:
        """Return the existing operation result, or None when it is not found."""
        ...


class ReconciliationRequired(RuntimeError):
    """Raised when an external operation may exist but cannot be safely retried."""


@dataclass(frozen=True)
class ReconciliationDecision:
    """Explicit application decision for an ambiguous provider outcome."""

    safe_to_retry: bool
    requires_reconciliation: bool
    reason: str


def decide_ambiguous_recovery(contract: ProviderExecutionContract) -> ReconciliationDecision:
    if contract.operation_safety is ProviderOperationSafety.IDEMPOTENT:
        return ReconciliationDecision(True, False, "provider guarantees idempotent operation reuse")
    if contract.operation_safety is ProviderOperationSafety.RECONCILABLE:
        return ReconciliationDecision(False, True, "provider requires reconciliation before retry")
    return ReconciliationDecision(False, True, "provider cannot safely resolve an ambiguous operation")
