from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest

from app.infrastructure.postgres_queue_health import PostgresQueueHealthReader


MIGRATION = Path(__file__).parents[2] / "migrations" / "0001_postgresql_durable_work.sql"


def test_postgres_queue_health_reports_due_work_expired_claims_and_dead_letters():
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        pytest.skip("DATABASE_URL is required for PostgreSQL integration tests")

    suffix = uuid4().hex
    job_id = f"queue-health-{suffix}"
    now = datetime.now(timezone.utc)
    reader = PostgresQueueHealthReader(lambda: psycopg.connect(database_url))
    before = reader.snapshot()
    rows = [
        # state, due offset, created offset, delivery attempt, claim state, error
        ("PENDING", -30, -45, 1, None, None),
        ("PENDING", 300, -10, 1, None, None),
        ("CLAIMED", -240, -300, 2, "expired", "LEASE_EXPIRED"),
        ("CLAIMED", -5, -20, 3, "active", None),
        ("DEAD", -600, -480, 5, None, "MAX_DELIVERY_ATTEMPTS_EXCEEDED"),
    ]

    with psycopg.connect(database_url) as connection:
        connection.execute(MIGRATION.read_text(encoding="utf-8"))
        connection.execute(
            """
            INSERT INTO generation_jobs (job_id, capability, idempotency_key, status)
            VALUES (%s, 'video', %s, 'QUEUED')
            """,
            (job_id, job_id + "-key"),
        )
        for state, due_offset, created_offset, attempt, claim_state, error in rows:
            claimed = claim_state is not None
            connection.execute(
                """
                INSERT INTO generation_work_items
                    (message_id, job_id, intent_key, due_at, state, delivery_attempt,
                     claim_token, claimed_by, claimed_until, last_error_code, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    uuid4(), job_id, f"health-intent-{uuid4().hex}",
                    now + timedelta(seconds=due_offset), state, attempt,
                    uuid4() if claimed else None,
                    f"worker-{suffix}" if claimed else None,
                    now + timedelta(seconds=-60 if claim_state == "expired" else 60)
                    if claimed else None,
                    error, now + timedelta(seconds=created_offset),
                ),
            )

    snapshot = reader.snapshot()

    # Use deltas because other integration tests may share this database.
    assert snapshot.pending_count - before.pending_count == 2
    assert snapshot.due_pending_count - before.due_pending_count == 1
    assert snapshot.active_claim_count - before.active_claim_count == 1
    assert snapshot.expired_claim_count - before.expired_claim_count == 1
    assert snapshot.dead_count - before.dead_count == 1
    assert snapshot.oldest_unresolved_age_seconds is not None
    assert snapshot.oldest_unresolved_age_seconds >= 450
    assert snapshot.oldest_due_pending_age_seconds is not None
    assert snapshot.oldest_due_pending_age_seconds >= 20
    assert snapshot.max_delivery_attempt >= 5
