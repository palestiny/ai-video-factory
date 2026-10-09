from __future__ import annotations

from dataclasses import asdict, is_dataclass
from enum import Enum
from typing import Any

from psycopg import Connection
from psycopg.types.json import Jsonb

from app.application.ports import GenerationResult
from app.application.provider_operation import ProviderOperation, ProviderOperationStatus
from app.application.provider_operation_repository import ProviderOperationRepository


class ProviderOperationVersionConflict(RuntimeError):
    """A concurrent writer changed this operation after it was read."""


def _json_value(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if is_dataclass(value):
        return _json_value(asdict(value))
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_json_value(item) for item in value]
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    raise TypeError(f"unsupported JSON value: {type(value).__name__}")


def _generation_result(value: Any) -> Any:
    if isinstance(value, dict) and {"provider", "artifact_refs"}.issubset(value):
        return GenerationResult(
            provider=value["provider"],
            provider_operation_id=value.get("provider_operation_id"),
            artifact_refs=tuple(value["artifact_refs"]),
            usage=value.get("usage", {}),
            cost=value.get("cost", {}),
            diagnostics=value.get("diagnostics", {}),
        )
    return value


class PostgresProviderOperationRepository(ProviderOperationRepository):
    """PostgreSQL adapter bound to a caller-owned transaction.

    Saves use optimistic compare-and-swap on version. The adapter never commits
    or rolls back; callers share this connection with work-intent persistence so
    state transitions and scheduled work can be atomic.
    """

    def __init__(self, connection: Connection) -> None:
        self._connection = connection

    def add(self, operation: ProviderOperation) -> None:
        if operation.version != 0:
            raise ValueError("new provider operation must start at version 0")
        self._connection.execute(
            """
            INSERT INTO provider_operations (
                provider, idempotency_key, operation_id, capability, status,
                submitted_at, terminal_result, failure_code, diagnostics,
                poll_generation, version
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                operation.provider, operation.idempotency_key, operation.operation_id,
                operation.capability, operation.status.value, operation.submitted_at,
                Jsonb(_json_value(operation.terminal_result))
                if operation.terminal_result is not None else None,
                operation.failure_code, Jsonb(_json_value(dict(operation.diagnostics))),
                operation.poll_generation, operation.version,
            ),
        )

    def get(self, provider: str, operation_id: str) -> ProviderOperation | None:
        row = self._connection.execute(
            """
            SELECT provider, operation_id, idempotency_key, capability, status,
                   submitted_at, terminal_result, failure_code, diagnostics,
                   poll_generation, version
            FROM provider_operations WHERE provider = %s AND operation_id = %s
            """,
            (provider, operation_id),
        ).fetchone()
        return self._from_row(row) if row is not None else None

    def get_by_idempotency_key(
        self, provider: str, idempotency_key: str
    ) -> ProviderOperation | None:
        row = self._connection.execute(
            """
            SELECT provider, operation_id, idempotency_key, capability, status,
                   submitted_at, terminal_result, failure_code, diagnostics,
                   poll_generation, version
            FROM provider_operations WHERE provider = %s AND idempotency_key = %s
            """,
            (provider, idempotency_key),
        ).fetchone()
        return self._from_row(row) if row is not None else None

    def save(self, operation: ProviderOperation) -> None:
        if operation.version < 1:
            raise ValueError("updated provider operation version must be positive")
        row = self._connection.execute(
            """
            UPDATE provider_operations
            SET status = %s, submitted_at = %s, terminal_result = %s,
                failure_code = %s, diagnostics = %s, poll_generation = %s,
                version = %s, updated_at = now()
            WHERE provider = %s AND operation_id = %s
              AND version = %s AND %s = version + 1
              AND (status NOT IN ('SUCCEEDED', 'FAILED', 'CANCELLED') OR status = %s)
            RETURNING version
            """,
            (
                operation.status.value, operation.submitted_at,
                Jsonb(_json_value(operation.terminal_result))
                if operation.terminal_result is not None else None,
                operation.failure_code, Jsonb(_json_value(dict(operation.diagnostics))),
                operation.poll_generation, operation.version, operation.provider,
                operation.operation_id, operation.version - 1, operation.version,
                operation.status.value,
            ),
        ).fetchone()
        if row is None:
            raise ProviderOperationVersionConflict(
                f"provider operation version conflict: "
                f"{operation.provider}/{operation.operation_id}@{operation.version - 1}"
            )

    @staticmethod
    def _from_row(row: tuple[Any, ...]) -> ProviderOperation:
        return ProviderOperation(
            provider=row[0], operation_id=row[1], idempotency_key=row[2],
            capability=row[3], status=ProviderOperationStatus(row[4]),
            submitted_at=row[5], terminal_result=_generation_result(row[6]),
            failure_code=row[7], diagnostics=row[8] or {},
            poll_generation=row[9], version=row[10],
        )
