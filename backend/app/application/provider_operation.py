from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Mapping


class ProviderOperationStatus(str, Enum):
    SUBMITTED = "SUBMITTED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    UNKNOWN = "UNKNOWN"


class ProviderCancellationStatus(str, Enum):
    ACCEPTED = "ACCEPTED"
    ALREADY_TERMINAL = "ALREADY_TERMINAL"
    UNSUPPORTED = "UNSUPPORTED"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True)
class ProviderOperation:
    provider: str
    operation_id: str
    idempotency_key: str
    capability: str
    status: ProviderOperationStatus = ProviderOperationStatus.SUBMITTED
    submitted_at: str | None = None

    def __post_init__(self) -> None:
        if not self.provider.strip():
            raise ValueError("provider cannot be blank")
        if not self.operation_id.strip():
            raise ValueError("operation_id cannot be blank")
        if not self.idempotency_key.strip():
            raise ValueError("idempotency_key cannot be blank")
        if not self.capability.strip():
            raise ValueError("capability cannot be blank")


@dataclass(frozen=True)
class ProviderOperationStatusResult:
    operation: ProviderOperation
    status: ProviderOperationStatus
    result: object | None = None
    failure_code: str | None = None
    diagnostics: Mapping[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class ProviderCancellationResult:
    status: ProviderCancellationStatus
    diagnostics: Mapping[str, object] = field(default_factory=dict)
