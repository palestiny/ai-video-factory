from datetime import datetime, timedelta, timezone
import os
from pathlib import Path

import psycopg
import pytest

from app.application.work_intent import WorkIntent
from app.infrastructure.postgres_work_intent import PostgresWorkIntentRepository


MIGRATION = Path(__file__).parents[2] / "migrations" / "0001_postgresql_durable_work.sql"


def _prepare(connection: psycopg.Connection) -> None:
    connection.execute(MIGRATION.read_text(encoding="utf-8"))
    connection.execute(
        """
        INSERT INTO generation_jobs
            (job_id, capability, idempotency_key, status)
        VALUES ('postgres-work-intent-job', 'video', 'postgres-work-intent-key', 'RUNNING')
        ON CONFLICT (job_id) DO NOTHING
        """
    )


def _intent(generation: int = 1, *, due_offset: int = 0) -> WorkIntent:
    return WorkIntent.provider_poll(
        job_id="postgres-work-intent-job",
        provider="provider-a",
        operation_id="postgres-operation",
        generation=generation,
        due_at=datetime(2026, 10, 9, 12, 0, tzinfo=timezone.utc)
        + timedelta(seconds=due_offset),
    )


def test_postgres_work_intent_repository_inserts_and_reads_without_committing():
    database_url = os.environ["DATABASE_URL"]

    with psycopg.connect(database_url) as connection:
        _prepare(connection)
        repository = PostgresWorkIntentRepository(connection)
        intent = _intent()

        assert repository.add_if_absent(intent) is True
        assert repository.get(intent.intent_key) == intent
        assert repository.add_if_absent(intent) is False

        connection.rollback()
        assert repository.get(intent.intent_key) is None


def test_postgres_work_intent_repository_rejects_same_key_with_different_contents():
    database_url = os.environ["DATABASE_URL"]

    with psycopg.connect(database_url) as connection:
        _prepare(connection)
        repository = PostgresWorkIntentRepository(connection)
        assert repository.add_if_absent(_intent()) is True

        with pytest.raises(ValueError, match="identity reused"):
            repository.add_if_absent(_intent(due_offset=15))

        connection.rollback()


def test_postgres_work_intent_repository_rolls_back_with_outer_transaction():
    database_url = os.environ["DATABASE_URL"]

    with psycopg.connect(database_url) as connection:
        _prepare(connection)
        repository = PostgresWorkIntentRepository(connection)
        intent = _intent()

        assert repository.add_if_absent(intent) is True
        connection.rollback()

    with psycopg.connect(database_url) as connection:
        _prepare(connection)
        assert PostgresWorkIntentRepository(connection).get(intent.intent_key) is None
