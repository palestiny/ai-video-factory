from __future__ import annotations

from uuid import uuid4

from psycopg import Connection

from app.application.work_intent import WorkIntent, WorkIntentRepository


class PostgresWorkIntentRepository(WorkIntentRepository):
    """PostgreSQL work-intent adapter bound to a caller-owned transaction.

    This repository deliberately never commits or rolls back. The transaction
    owner must share this connection with operation-state persistence so intent
    creation can be atomic with the state transition that requires it.
    """

    def __init__(self, connection: Connection) -> None:
        self._connection = connection

    def add_if_absent(self, intent: WorkIntent) -> bool:
        inserted = self._connection.execute(
            """
            INSERT INTO generation_work_items (
                message_id,
                job_id,
                intent_key,
                intent_kind,
                provider,
                operation_id,
                generation,
                due_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (intent_key) DO NOTHING
            RETURNING intent_key
            """,
            (
                str(uuid4()),
                intent.job_id,
                intent.intent_key,
                intent.kind,
                intent.provider,
                intent.operation_id,
                intent.generation,
                intent.due_at,
            ),
        ).fetchone()

        if inserted is not None:
            return True

        existing = self.get(intent.intent_key)
        if existing is None:
            # A different database uniqueness constraint (for example, a
            # provider/operation/generation collision) may have rejected it.
            raise ValueError(
                "work intent conflicts with an existing provider poll generation"
            )
        if existing != intent:
            raise ValueError(
                f"work intent identity reused with different contents: {intent.intent_key}"
            )
        return False

    def get(self, intent_key: str) -> WorkIntent | None:
        row = self._connection.execute(
            """
            SELECT intent_key, job_id, due_at, intent_kind,
                   provider, operation_id, generation
            FROM generation_work_items
            WHERE intent_key = %s
            """,
            (intent_key,),
        ).fetchone()
        if row is None:
            return None
        return WorkIntent(
            intent_key=row[0],
            job_id=row[1],
            due_at=row[2],
            kind=row[3],
            provider=row[4],
            operation_id=row[5],
            generation=row[6],
        )
