from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
from uuid import uuid4

import psycopg

from app.infrastructure.postgres_worker_lease import PostgresWorkerLeaseRepository

MIGRATION = Path(__file__).parents[2] / "migrations" / "0001_postgresql_durable_work.sql"


def _setup() -> tuple[str, PostgresWorkerLeaseRepository]:
    database_url = os.environ["DATABASE_URL"]
    job_id = "worker-lease-" + uuid4().hex
    with psycopg.connect(database_url) as connection:
        connection.execute(MIGRATION.read_text(encoding="utf-8"))
        connection.execute(
            """
            INSERT INTO generation_jobs (job_id, capability, idempotency_key, status)
            VALUES (%s, 'video', %s, 'QUEUED')
            """,
            (job_id, job_id + "-key"),
        )
    return job_id, PostgresWorkerLeaseRepository(lambda: psycopg.connect(database_url))


def test_only_one_active_worker_lease_can_be_claimed():
    job_id, leases = _setup()
    now = datetime.now(timezone.utc)
    first = leases.claim(job_id, "worker-a", now, timedelta(seconds=30))
    second = leases.claim(job_id, "worker-b", now, timedelta(seconds=30))

    assert first is not None
    assert second is None
    assert leases.current(job_id) == first


def test_expired_lease_is_reclaimed_with_fresh_token_and_old_token_is_fenced():
    job_id, leases = _setup()
    now = datetime.now(timezone.utc)
    first = leases.claim(job_id, "worker-a", now, timedelta(seconds=1))
    assert first is not None

    second = leases.claim(job_id, "worker-b", now + timedelta(seconds=2), timedelta(seconds=30))
    assert second is not None
    assert second.lease_token != first.lease_token
    assert leases.renew(job_id, first.lease_token, now + timedelta(seconds=3), timedelta(seconds=30)) is None
    assert leases.release(job_id, first.lease_token) is False
    assert leases.current(job_id) == second


def test_expired_lease_can_be_recovered_explicitly():
    job_id, leases = _setup()
    now = datetime.now(timezone.utc)
    lease = leases.claim(job_id, "worker-a", now, timedelta(seconds=1))
    assert lease is not None

    assert leases.recover_expired(job_id, now + timedelta(seconds=2)) is True
    assert leases.current(job_id) is None
    assert leases.recover_expired(job_id, now + timedelta(seconds=3)) is False
