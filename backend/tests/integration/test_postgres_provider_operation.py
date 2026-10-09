from dataclasses import replace
from datetime import datetime, timedelta, timezone
import os
from pathlib import Path

import psycopg
import pytest

from app.application.ports import GenerationResult
from app.application.provider_operation import ProviderOperation, ProviderOperationStatus
from app.application.work_intent import WorkIntent
from app.infrastructure.postgres_provider_operation import (
    PostgresProviderOperationRepository,
    ProviderOperationVersionConflict,
)
from app.infrastructure.postgres_work_intent import PostgresWorkIntentRepository


MIGRATION = Path(__file__).parents[2] / "migrations" / "0001_postgresql_durable_work.sql"
PROVIDER = "postgres-provider-operation-test"
OPERATION_ID = "postgres-operation-test"
JOB_ID = "postgres-work-intent-job"


def _prepare() -> None:
    with psycopg.connect(os.environ["DATABASE_URL"]) as connection:
        connection.execute(MIGRATION.read_text(encoding="utf-8"))
        connection.execute(
            """
            INSERT INTO generation_jobs (job_id, capability, idempotency_key, status)
            VALUES (%s, 'video', %s, 'RUNNING')
            ON CONFLICT (job_id) DO NOTHING
            """,
            (JOB_ID, JOB_ID + "-key"),
        )
        connection.execute(
            "DELETE FROM generation_work_items WHERE provider = %s AND operation_id = %s",
            (PROVIDER, OPERATION_ID),
        )
        connection.execute(
            "DELETE FROM provider_operations WHERE provider = %s AND operation_id = %s",
            (PROVIDER, OPERATION_ID),
        )
        connection.execute(
            "DELETE FROM provider_operations WHERE provider = %s AND idempotency_key = %s",
            (PROVIDER, OPERATION_ID + "-key"),
        )
        PostgresProviderOperationRepository(connection).add(_operation())


def _operation() -> ProviderOperation:
    return ProviderOperation(
        provider=PROVIDER,
        operation_id=OPERATION_ID,
        idempotency_key=OPERATION_ID + "-key",
        capability="video",
    )


def _intent(generation: int = 1) -> WorkIntent:
    return WorkIntent.provider_poll(
        job_id=JOB_ID,
        provider=PROVIDER,
        operation_id=OPERATION_ID,
        generation=generation,
        due_at=datetime.now(timezone.utc) + timedelta(seconds=30),
    )


def test_provider_operation_round_trip_and_result_normalization():
    _prepare()
    expected_result = GenerationResult(
        provider=PROVIDER,
        provider_operation_id=OPERATION_ID,
        artifact_refs=("artifact://video/1",),
        usage={"seconds": 3},
    )
    with psycopg.connect(os.environ["DATABASE_URL"]) as connection:
        repository = PostgresProviderOperationRepository(connection)
        current = repository.get(PROVIDER, OPERATION_ID)
        assert current is not None and current.version == 0
        updated = replace(
            current,
            status=ProviderOperationStatus.SUCCEEDED,
            terminal_result=expected_result,
            version=1,
        )
        repository.save(updated)

    with psycopg.connect(os.environ["DATABASE_URL"]) as connection:
        stored = PostgresProviderOperationRepository(connection).get(PROVIDER, OPERATION_ID)
        assert stored is not None
        assert stored.status is ProviderOperationStatus.SUCCEEDED
        assert stored.version == 1
        assert stored.terminal_result == expected_result


def test_operation_state_and_poll_intent_rollback_together():
    _prepare()
    intent = _intent()
    with psycopg.connect(os.environ["DATABASE_URL"]) as connection:
        operations = PostgresProviderOperationRepository(connection)
        current = operations.get(PROVIDER, OPERATION_ID)
        assert current is not None
        operations.save(replace(
            current,
            status=ProviderOperationStatus.RUNNING,
            poll_generation=1,
            version=1,
        ))
        assert PostgresWorkIntentRepository(connection).add_if_absent(intent)
        connection.rollback()

    with psycopg.connect(os.environ["DATABASE_URL"]) as connection:
        stored = PostgresProviderOperationRepository(connection).get(PROVIDER, OPERATION_ID)
        assert stored is not None
        assert stored.status is ProviderOperationStatus.SUBMITTED
        assert stored.poll_generation == 0
        assert stored.version == 0
        assert PostgresWorkIntentRepository(connection).get(intent.intent_key) is None


def test_operation_state_and_poll_intent_commit_together():
    _prepare()
    intent = _intent()
    with psycopg.connect(os.environ["DATABASE_URL"]) as connection:
        operations = PostgresProviderOperationRepository(connection)
        current = operations.get(PROVIDER, OPERATION_ID)
        assert current is not None
        operations.save(replace(
            current,
            status=ProviderOperationStatus.RUNNING,
            poll_generation=1,
            version=1,
        ))
        assert PostgresWorkIntentRepository(connection).add_if_absent(intent)

    with psycopg.connect(os.environ["DATABASE_URL"]) as connection:
        stored = PostgresProviderOperationRepository(connection).get(PROVIDER, OPERATION_ID)
        assert stored is not None
        assert stored.status is ProviderOperationStatus.RUNNING
        assert stored.poll_generation == 1
        assert stored.version == 1
        assert PostgresWorkIntentRepository(connection).get(intent.intent_key) == intent


def test_stale_concurrent_provider_operation_update_is_rejected():
    _prepare()
    database_url = os.environ["DATABASE_URL"]
    with psycopg.connect(database_url) as first, psycopg.connect(database_url) as second:
        first_repo = PostgresProviderOperationRepository(first)
        second_repo = PostgresProviderOperationRepository(second)
        first_read = first_repo.get(PROVIDER, OPERATION_ID)
        second_read = second_repo.get(PROVIDER, OPERATION_ID)
        assert first_read is not None and second_read is not None

        first_repo.save(replace(first_read, status=ProviderOperationStatus.RUNNING, version=1))
        first.commit()
        with pytest.raises(ProviderOperationVersionConflict):
            second_repo.save(replace(second_read, status=ProviderOperationStatus.SUCCEEDED, version=1))
        second.rollback()

    with psycopg.connect(database_url) as connection:
        stored = PostgresProviderOperationRepository(connection).get(PROVIDER, OPERATION_ID)
        assert stored is not None
        assert stored.status is ProviderOperationStatus.RUNNING
        assert stored.version == 1
