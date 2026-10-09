from datetime import datetime, timedelta, timezone
import os
from pathlib import Path

import psycopg
import pytest

from app.application.work_intent import WorkIntent
from app.infrastructure.postgres_generation_job_queue import PostgresGenerationJobQueue
from app.infrastructure.postgres_work_intent import PostgresWorkIntentRepository


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


def test_postgres_queue_claim_preserves_provider_poll_identity():
    _prepare()
    now = datetime.now(timezone.utc)
    intent = WorkIntent.provider_poll(
        job_id="postgres-queue-job",
        provider="provider-a",
        operation_id="operation-a",
        generation=4,
        due_at=now - timedelta(seconds=1),
    )
    with psycopg.connect(os.environ["DATABASE_URL"]) as connection:
        assert PostgresWorkIntentRepository(connection).add_if_absent(intent)

    message = _queue().claim_next("worker-a", now, timedelta(seconds=30))

    assert message is not None
    assert message.intent_key == intent.intent_key
    assert message.intent_kind == "PROVIDER_POLL"
    assert message.provider == "provider-a"
    assert message.operation_id == "operation-a"
    assert message.generation == 4


def test_postgres_queue_rejects_ack_and_release_after_lease_expiry():
    _prepare()
    queue = _queue()
    queued = queue.enqueue("postgres-queue-job")
    now = datetime.now(timezone.utc)
    message = queue.claim_next("worker-a", now, timedelta(minutes=5))
    assert message is not None and message.message_id == queued.message_id

    with psycopg.connect(os.environ["DATABASE_URL"]) as connection:
        connection.execute(
            """
            UPDATE generation_work_items
            SET claimed_until = now() - interval '1 second'
            WHERE message_id = %s
            """,
            (message.message_id,),
        )

    with pytest.raises(ValueError, match="stale or unowned"):
        queue.ack(message)
    with pytest.raises(ValueError, match="stale or unowned"):
        queue.release_or_requeue(message)

    # Expired work remains recoverable; reclaiming fences out the old token.
    reclaimed = queue.claim_next(
        "worker-b", now + timedelta(minutes=6), timedelta(minutes=1)
    )
    assert reclaimed is not None
    assert reclaimed.message_id == message.message_id
    assert reclaimed.claim_token != message.claim_token
    queue.ack(reclaimed)
