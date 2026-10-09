from __future__ import annotations

from collections.abc import Callable

from psycopg import Connection

from app.infrastructure.postgres_persistence import PostgresPersistenceTransaction


class ManagedPostgresPersistenceTransaction(PostgresPersistenceTransaction):
    """A transaction adapter that owns and closes its psycopg connection."""

    def __init__(self, connection: Connection, *, idempotency_scope: str = "generation") -> None:
        super().__init__(connection, idempotency_scope=idempotency_scope)
        self._connection_closed = False

    def close(self) -> None:
        """Rollback any uncommitted work and close the owned connection."""
        if self._connection_closed:
            return
        try:
            self.rollback()
        finally:
            self._connection_closed = True
            self._connection.close()


class PostgresPersistenceTransactionFactory:
    """Callable factory for worker deliveries.

    One transaction object may be committed multiple times during a single
    async delivery (submission, poll observation, and scheduling). Its
    connection therefore remains open until the worker's delivery-finally
    cleanup calls close().
    """

    def __init__(
        self,
        connection_factory: Callable[[], Connection],
        *,
        idempotency_scope: str = "generation",
    ) -> None:
        self._connection_factory = connection_factory
        self._idempotency_scope = idempotency_scope

    def __call__(self) -> ManagedPostgresPersistenceTransaction:
        return ManagedPostgresPersistenceTransaction(
            self._connection_factory(),
            idempotency_scope=self._idempotency_scope,
        )
