from datetime import datetime, timedelta, timezone
import os
from pathlib import Path

import psycopg
import pytest

from app.infrastructure.postgres_generation_job_queue import PostgresGenerationJobQueue


MIGRATION = Path(__file__).parents[2] / "migrations" / "0001_postgresql_durable_work.sql"


def _queue() -> PostgresGenerationJobQueue:
    database_url = os.environ["DATABASE_URL"]
    return PostgresGenerationJobQueue(lambda: psycopg.connect(database_url))


def _prepare() -> None:
    database_url = os.environ["DATABASE_URL"]
    with psycopg.connect(database_url) as connection:
        connection.execute(MIGRATION.read_text(encoding="utf-8"))
        connection.execute(
            """
            INSERT INTO generation_jobs
                (job_id, capability, idempotency_key, status)
            VALUES ('postgres-queue-job', 'video', 'postgres-queue-key', 'QUEUED')
            ON CONFLICT (job_id) DO NOTHING
            """
        )
        connection.execute(
            "DELETE FROM generation_work_items WHERE job_id = 'postgres-queue-job'"
        )


def test_postgres_queue_claims_due_work_and_acknowledges_with_claim_token():
    _prepare()
    queue = _queue()
    queued = queue.enqueue("postgres-queue-job")
    message = queue.claim_next(
        "worker-a", datetime.now(timezone.utc), timedelta(seconds=30)
    )

    assert message is not None
    assert message.message_id == queued.message_id
    assert message.claim_token

    queue.ack(message)
    with psycopg.connect(os.environ["DATABASE_URL"]) as connection:
        row = connection.execute(
            "SELECT state, claim_token, acked_at FROM generation_work_items WHERE message_id = %s",
            (message.message_id,),
        ).fetchone()
    assert row[0] == "ACKED"
    assert row[1] is None
    assert row[2] is not None


def test_postgres_queue_recovers_expired_claim_and_rejects_stale_ack():
    _prepare()
    queue = _queue()
    queued = queue.enqueue("postgres-queue-job")
    now = datetime.now(timezone.utc)
    first = queue.claim_next("worker-a", now, timedelta(seconds=1))
    assert first is not None and first.message_id == queued.message_id

    second = queue.claim_next(
        "worker-b", now + timedelta(seconds=2), timedelta(seconds=30)
    )
    assert second is not None
    assert second.message_id == first.message_id
    assert second.delivery_attempt == first.delivery_attempt + 1
    assert second.claim_token != first.claim_token

    with pytest.raises(ValueError, match="stale or unowned"):
        queue.ack(first)

    queue.ack(second)


def test_postgres_queue_does_not_claim_future_work():
    _prepare()
    queue = _queue()
    queue.enqueue_after("postgres-queue-job", timedelta(hours=1))

    message = queue.claim_next(
        "worker-a", datetime.now(timezone.utc), timedelta(seconds=30)
    )
    assert message is None
