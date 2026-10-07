from __future__ import annotations

from dataclasses import dataclass, replace

from app.application.persistence import ExecutionPersistenceTransaction
from app.application.ports import GenerationRequest, GenerationResult, ProviderOperationPort
from app.application.provider_operation import (
    ProviderCancellationResult,
    ProviderCancellationStatus,
    ProviderOperation,
    ProviderOperationStatus,
    ProviderOperationStatusResult,
)
from app.application.reconciliation import AmbiguousProviderOutcome, ProviderExecutionContract


@dataclass(frozen=True)
class ProviderOperationPoll:
    operation: ProviderOperation
    status: ProviderOperationStatus
    generation: GenerationResult | None = None


class ProviderOperationLifecycle:
    """Durable application orchestration for long-running provider operations.

    External submission happens before local persistence, so a persistence
    failure after a successful provider submission is deliberately surfaced as
    an ambiguous outcome instead of being converted into a blind retry.
    """

    def __init__(
        self,
        transaction: ExecutionPersistenceTransaction,
        provider: ProviderOperationPort,
        provider_name: str,
        contract: ProviderExecutionContract,
    ) -> None:
        if not provider_name.strip():
            raise ValueError("provider_name cannot be blank")
        if contract.provider != provider_name:
            raise ValueError("provider contract/provider name mismatch")
        self._transaction = transaction
        self._provider = provider
        self._provider_name = provider_name
        self._contract = contract

    def submit(self, request: GenerationRequest) -> ProviderOperation:
        existing = self._transaction.provider_operations.get_by_idempotency_key(
            self._provider_name,
            request.idempotency_key,
        )
        if existing is not None:
            return existing

        try:
            operation = self._provider.submit(request)
        except AmbiguousProviderOutcome:
            raise

        self._validate_operation(operation, request)

        try:
            self._transaction.provider_operations.add(operation)
            self._transaction.commit()
        except Exception as exc:
            self._transaction.rollback()
            raise AmbiguousProviderOutcome(
                provider=self._provider_name,
                contract=self._contract,
                message="provider submission succeeded but durable operation persistence is uncertain",
            ) from exc
        return operation

    def poll(self, operation: ProviderOperation) -> ProviderOperationStatusResult:
        current = self._transaction.provider_operations.get(
            operation.provider,
            operation.operation_id,
        )
        if current is None:
            raise KeyError(
                f"provider operation not found: {(operation.provider, operation.operation_id)}"
            )

        try:
            status = self._provider.get_status(current)
        except AmbiguousProviderOutcome:
            raise

        self._validate_status(status, current)
        updated = replace(current, status=status.status)

        try:
            self._transaction.provider_operations.save(updated)
            self._transaction.commit()
        except Exception as exc:
            self._transaction.rollback()
            raise RuntimeError(
                "provider status was observed but durable operation state is uncertain"
            ) from exc

        return ProviderOperationStatusResult(
            operation=updated,
            status=status.status,
            result=status.result,
            failure_code=status.failure_code,
            diagnostics=status.diagnostics,
        )

    def cancel(self, operation: ProviderOperation) -> ProviderCancellationResult:
        current = self._transaction.provider_operations.get(
            operation.provider,
            operation.operation_id,
        )
        if current is None:
            raise KeyError(
                f"provider operation not found: {(operation.provider, operation.operation_id)}"
            )

        if current.status in {
            ProviderOperationStatus.SUCCEEDED,
            ProviderOperationStatus.FAILED,
            ProviderOperationStatus.CANCELLED,
        }:
            return ProviderCancellationResult(
                status=ProviderCancellationStatus.ALREADY_TERMINAL
            )

        return self._provider.cancel(current)

    def _validate_operation(
        self,
        operation: ProviderOperation,
        request: GenerationRequest,
    ) -> None:
        if operation.provider != self._provider_name:
            raise ValueError("provider operation returned unexpected provider")
        if operation.idempotency_key != request.idempotency_key:
            raise ValueError("provider operation returned unexpected idempotency key")
        if operation.capability != request.capability:
            raise ValueError("provider operation returned unexpected capability")

    @staticmethod
    def _validate_status(
        result: ProviderOperationStatusResult,
        current: ProviderOperation,
    ) -> None:
        if result.operation.provider != current.provider:
            raise ValueError("provider status returned unexpected provider")
        if result.operation.operation_id != current.operation_id:
            raise ValueError("provider status returned unexpected operation identity")
        if result.operation.idempotency_key != current.idempotency_key:
            raise ValueError("provider status returned unexpected idempotency key")
