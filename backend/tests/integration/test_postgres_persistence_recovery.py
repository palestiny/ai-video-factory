from dataclasses import replace
from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest

from app.application.provider_operation import ProviderOperation, ProviderOperationStatus
from app.application.work_intent import WorkIntent
from app.infrastructure.postgres_generation_job_queue import PostgresGenerationJobQueue
from app.infrastructure.postgres_persistence import PostgresPersistenceTransaction
from app.infrastructure.postgres_provider_operation import PostgresProviderOperationRepository


MIGRATION = Path(__file__).parents[2] / "migrations" / "0001_postgresql_durable_work.sql"


def test_committed_poll_intent_survives_crash_before_ack_and_source_is_reclaimed():
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        pytest.skip("DATABASE_URL is required for PostgreSQL integration tests")

    job_id = "recovery-job-" + uuid4().hex
    provider = "recovery-provider-" + uuid4().hex
    operation_id = "recovery-operation-" + uuid4().hex
    source_key = "recovery-source-" + uuid4().hex

    with psycopg.connect(database_url) as connection:
        connection.execute(MIGRATION.read_text(encoding="utf-8"))
        connection.execute(
            """
            INSERT INTO generation_jobs (job_id, capability, idempotency_key, status)
            VALUES (%s, 'video', %s, 'RUNNING')
            """,
            (job_id, job_id + "-key"),
        )
        PostgresProviderOperationRepository(connection).add(
            ProviderOperation(
                provider=provider,
                operation_id=operation_id,
                idempotency_key=job_id + "-provider-key",
                capability="video",
            )
        )

    queue = PostgresGenerationJobQueue(lambda: psycopg.connect(database_url))
    source = queue.enqueue(job_id)
    claimed = queue.claim_next(
        "worker-before-crash",
        datetime.now(timezone.utc),
        timedelta(minutes=2),
    )
    assert claimed is not None
    assert claimed.message_id == source.message_id
    assert claimed.claim_token is not None

    intent = WorkIntent.provider_poll(
        job_id=job_id,
        provider=provider,
        operation_id=operation_id,
        generation=1,
        due_at=datetime.now(timezone.utc) + timedelta(minutes=1),
    )

    # This is the durable commit immediately before the simulated process crash.
    # The source delivery is deliberately not acknowledged.
    with psycopg.connect(database_url) as connection:
        transaction = PostgresPersistenceTransaction(connection)
        operation = transaction.provider_operations.get(provider, operation_id)
        assert operation is not None
        transaction.provider_operations.save(
            replace(
                operation,
                status=ProviderOperationStatus.RUNNING,
                poll_generation=1,
                version=1,
            )
        )
        assert transaction.work_intents.add_if_absent(intent)
        transaction.commit()

    # Simulate the worker process disappearing: its queue lease expires while
    # its already-committed database state remains durable.
    with psycopg.connect(database_url) as connection:
        connection.execute(
            """
            UPDATE generation_work_items
            SET claimed_until = now() - interval '1 second'
            WHERE message_id = %s
            """,
            (claimed.message_id,),
        )

    # A fresh queue instance models a restarted process with no in-memory state.
    restarted_queue = PostgresGenerationJobQueue(lambda: psycopg.connect(database_url))
    recovered = restarted_queue.claim_next(
        "worker-after-restart",
        datetime.now(timezone.utc),
        timedelta(minutes=2),
    )
    assert recovered is not None
    assert recovered.message_id == claimed.message_id
    assert recovered.claim_token != claimed.claim_token

    with psycopg.connect(database_url) as connection:
        transaction = PostgresPersistenceTransaction(connection)
        stored_operation = transaction.provider_operations.get(provider, operation_id)
        assert stored_operation is not None
        assert stored_operation.status is ProviderOperationStatus.RUNNING
        assert stored_operation.poll_generation == 1
        assert transaction.work_intents.get(intent.intent_key) == intent

    # Old ownership is fenced; the restarted worker can safely acknowledge.
    with pytest.raises(ValueError, match="stale or unowned"):
        restarted_queue.ack(claimed)
    restarted_queue.ack(recovered)
