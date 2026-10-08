"""Real-PostgreSQL smoke tests for the initial durable-work schema.

The CI workflow provides an isolated PostgreSQL database. These tests verify
that the schema applies and key database-enforced invariants exist; they do
not claim to test a production repository, queue adapter, or claim protocol.
"""
from pathlib import Path
import os

import psycopg
import pytest


MIGRATION = Path(__file__).parents[2] / "migrations" / "0001_postgresql_durable_work.sql"


def _apply_migration(connection: psycopg.Connection) -> None:
    connection.execute(MIGRATION.read_text(encoding="utf-8"))


def test_durable_work_migration_applies_and_creates_required_tables():
    database_url = os.environ["DATABASE_URL"]

    with psycopg.connect(database_url, autocommit=True) as connection:
        _apply_migration(connection)
        rows = connection.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_type = 'BASE TABLE'
            """
        ).fetchall()

    tables = {row[0] for row in rows}
    assert {
        "generation_jobs",
        "idempotency_reservations",
        "generation_attempt_versions",
        "provider_operations",
        "job_events",
        "generation_work_items",
    } <= tables


def test_work_intent_key_is_database_unique():
    """A repeated logical intent must not become a second queue row."""
    database_url = os.environ["DATABASE_URL"]

    with psycopg.connect(database_url, autocommit=True) as connection:
        _apply_migration(connection)
        connection.execute(
            """
            INSERT INTO generation_jobs
                (job_id, capability, idempotency_key, status)
            VALUES ('schema-constraint-job', 'video', 'schema-constraint-key', 'RUNNING')
            ON CONFLICT (job_id) DO NOTHING
            """
        )
        connection.execute(
            """
            INSERT INTO generation_work_items
                (message_id, job_id, intent_key, due_at)
            VALUES (
                '00000000-0000-0000-0000-000000000001',
                'schema-constraint-job',
                'provider-op-1:poll:1',
                now()
            )
            """
        )

        with pytest.raises(psycopg.errors.UniqueViolation):
            connection.execute(
                """
                INSERT INTO generation_work_items
                    (message_id, job_id, intent_key, due_at)
                VALUES (
                    '00000000-0000-0000-0000-000000000002',
                    'schema-constraint-job',
                    'provider-op-1:poll:1',
                    now()
                )
                """
            )


def test_claimed_work_requires_a_lease_token_and_expiry():
    """The database must reject a CLAIMED row that cannot be fenced/recovered."""
    database_url = os.environ["DATABASE_URL"]

    with psycopg.connect(database_url, autocommit=True) as connection:
        _apply_migration(connection)
        connection.execute(
            """
            INSERT INTO generation_jobs
                (job_id, capability, idempotency_key, status)
            VALUES ('schema-claim-job', 'video', 'schema-claim-key', 'RUNNING')
            ON CONFLICT (job_id) DO NOTHING
            """
        )

        with pytest.raises(psycopg.errors.CheckViolation):
            connection.execute(
                """
                INSERT INTO generation_work_items
                    (message_id, job_id, intent_key, due_at, state)
                VALUES (
                    '00000000-0000-0000-0000-000000000003',
                    'schema-claim-job',
                    'provider-op-claim:poll:1',
                    now(),
                    'CLAIMED'
                )
                """
            )


def test_non_claimed_work_cannot_retain_stale_lease_metadata():
    """ACK/requeue transitions must clear lease ownership rather than leave stale tokens."""
    database_url = os.environ["DATABASE_URL"]

    with psycopg.connect(database_url, autocommit=True) as connection:
        _apply_migration(connection)
        connection.execute(
            """
            INSERT INTO generation_jobs
                (job_id, capability, idempotency_key, status)
            VALUES ('schema-stale-lease-job', 'video', 'schema-stale-lease-key', 'RUNNING')
            ON CONFLICT (job_id) DO NOTHING
            """
        )

        with pytest.raises(psycopg.errors.CheckViolation):
            connection.execute(
                """
                INSERT INTO generation_work_items
                    (message_id, job_id, intent_key, due_at, state, claim_token)
                VALUES (
                    '00000000-0000-0000-0000-000000000004',
                    'schema-stale-lease-job',
                    'provider-op-stale-lease:poll:1',
                    now(),
                    'PENDING',
                    '00000000-0000-0000-0000-000000000005'
                )
                """
            )



def test_provider_operation_schema_has_nonnegative_poll_generation():
    database_url = os.environ["DATABASE_URL"]

    with psycopg.connect(database_url, autocommit=True) as connection:
        _apply_migration(connection)
        column = connection.execute(
            """
            SELECT column_default, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'provider_operations'
              AND column_name = 'poll_generation'
            """
        ).fetchone()

        assert column is not None
        assert column[0] is not None and "0" in column[0]
        assert column[1] == "NO"

        with pytest.raises(psycopg.errors.CheckViolation):
            connection.execute(
                """
                INSERT INTO provider_operations
                    (provider, idempotency_key, operation_id, capability, status, poll_generation)
                VALUES ('schema-provider', 'schema-key', 'schema-op', 'video', 'RUNNING', -1)
                """
            )
