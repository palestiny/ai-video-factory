from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class FailureCode(str, Enum):
    INVALID_REQUEST = "INVALID_REQUEST"
    AUTHENTICATION = "AUTHENTICATION"
    RATE_LIMITED = "RATE_LIMITED"
    TIMEOUT = "TIMEOUT"
    PROVIDER_FAILURE = "PROVIDER_FAILURE"
    CONTENT_REJECTED = "CONTENT_REJECTED"
    TRANSIENT_NETWORK = "TRANSIENT_NETWORK"
    UNKNOWN = "UNKNOWN"
    RECONCILIATION_REQUIRED = "RECONCILIATION_REQUIRED"


_RETRYABLE = {
    FailureCode.RATE_LIMITED,
    FailureCode.TIMEOUT,
    FailureCode.PROVIDER_FAILURE,
    FailureCode.TRANSIENT_NETWORK,
}


@dataclass(frozen=True)
class Failure:
    code: FailureCode
    message: str
    provider: str | None = None
    provider_operation_id: str | None = None
    diagnostics: str | None = None

    def __post_init__(self) -> None:
        if not self.message.strip():
            raise ValueError("failure message cannot be blank")
        if self.provider is not None and not self.provider.strip():
            raise ValueError("provider cannot be blank when supplied")

    @property
    def retryable(self) -> bool:
        return self.code in _RETRYABLE

    @classmethod
    def from_code(
        cls,
        code: str,
        message: str,
        *,
        provider: str | None = None,
        provider_operation_id: str | None = None,
        diagnostics: str | None = None,
    ) -> "Failure":
        normalized = code.strip().upper()
        try:
            failure_code = FailureCode(normalized)
        except ValueError:
            failure_code = FailureCode.UNKNOWN

        return cls(
            code=failure_code,
            message=message.strip(),
            provider=provider.strip() if provider is not None else None,
            provider_operation_id=provider_operation_id.strip()
            if provider_operation_id is not None
            else None,
            diagnostics=diagnostics,
        )
