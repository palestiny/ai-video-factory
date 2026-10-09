from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest

from app.application.execution import LeaseOwnershipLost
from app.application.provider_operation import ProviderOperation, ProviderOperationStatus
from app.application.work_intent import WorkIntent
from app.domain.events import JobEvent, JobEventType
from app.domain.generation import GenerationStatus
from app.infrastructure.postgres_persistence import PostgresPersistenceTransaction


MIGRATION = Path(__file__).parents[2] / "migrations" / "0001_postgresql_durable_work.sql"


def _connect():
    return psycopg.connect(os.environ["DATABASE_URL"])


def _prepare() -> tuple[str, str, str]:
    job_id = f"tx-job-{uuid4()}"
    provider = f"tx-provider-{uuid4()}"
    operation_id = f"tx-operation-{uuid4()}"
    with _connect() as connection:
        connection.execute(MIGRATION.read_text(encoding="utf-8"))
        connection.execute(
            """
            INSERT INTO generation_jobs (job_id, capability, idempotency_key, status)
            VALUES (%s, 'video', %s, 'QUEUED')
            """,
            (job_id, f"{job_id}-key"),
        )
    return job_id, provider, operation_id


def _operation(job_id: str, provider: str, operation_id: str) -> ProviderOperation:
    return ProviderOperation(
        provider=provider,
        operation_id=operation_id,
        idempotency_key=f"{job_id}-provider-key",
        capability="video",
    )


def test_shared_transaction_commits_job_event_operation_and_poll_intent():
    job_id, provider, operation_id = _prepare()
    due_at = datetime.now(timezone.utc) + timedelta(seconds=20)
    intent = WorkIntent.provider_poll(
        job_id=job_id, provider=provider, operation_id=operation_id,
        generation=1, due_at=due_at,
    )
    with _connect() as connection:
        tx = PostgresPersistenceTransaction(connection)
        job = tx.jobs.get(job_id)
        assert job is not None
        job.status = GenerationStatus.RUNNING
        tx.jobs.save(job)
        tx.provider_operations.add(_operation(job_id, provider, operation_id))
        assert tx.work_intents.add_if_absent(intent)
        tx.append_event(JobEvent(
            event_id=f"{job_id}:running", job_id=job_id,
            event_type=JobEventType.STARTED, occurred_at=datetime.now(timezone.utc),
            attempt_number=1, metadata=(("source", "integration-test"),),
        ))
        tx.commit()

    with _connect() as connection:
        assert connection.execute(
            "SELECT status FROM generation_jobs WHERE job_id = %s", (job_id,)
        ).fetchone()[0] == "RUNNING"
        assert connection.execute(
            "SELECT count(*) FROM provider_operations WHERE provider = %s AND operation_id = %s",
            (provider, operation_id),
        ).fetchone()[0] == 1
        assert connection.execute(
            "SELECT count(*) FROM generation_work_items WHERE intent_key = %s",
            (intent.intent_key,),
        ).fetchone()[0] == 1
        event = connection.execute(
            "SELECT event_type, metadata FROM job_events WHERE event_id = %s",
            (f"{job_id}:running",),
        ).fetchone()
        assert event[0] == "STARTED"
        assert event[1]["source"] == "integration-test"


def test_shared_transaction_rolls_back_every_repository_write():
    job_id, provider, operation_id = _prepare()
    intent = WorkIntent.provider_poll(
        job_id=job_id, provider=provider, operation_id=operation_id,
        generation=1, due_at=datetime.now(timezone.utc) + timedelta(seconds=20),
    )
    with _connect() as connection:
        tx = PostgresPersistenceTransaction(connection)
        job = tx.jobs.get(job_id)
        assert job is not None
        job.status = GenerationStatus.RUNNING
        tx.jobs.save(job)
        tx.provider_operations.add(_operation(job_id, provider, operation_id))
        tx.work_intents.add_if_absent(intent)
        tx.append_event(JobEvent(
            event_id=f"{job_id}:rollback", job_id=job_id,
            event_type=JobEventType.STARTED, occurred_at=datetime.now(timezone.utc),
            attempt_number=1,
        ))
        tx.rollback()

    with _connect() as connection:
        assert connection.execute(
            "SELECT status FROM generation_jobs WHERE job_id = %s", (job_id,)
        ).fetchone()[0] == "QUEUED"
        assert connection.execute(
            "SELECT count(*) FROM provider_operations WHERE provider = %s AND operation_id = %s",
            (provider, operation_id),
        ).fetchone()[0] == 0
        assert connection.execute(
            "SELECT count(*) FROM generation_work_items WHERE intent_key = %s",
            (intent.intent_key,),
        ).fetchone()[0] == 0
        assert connection.execute(
            "SELECT count(*) FROM job_events WHERE event_id = %s",
            (f"{job_id}:rollback",),
        ).fetchone()[0] == 0


def test_lease_owner_is_checked_again_at_commit():
    job_id, _, _ = _prepare()
    worker_id = f"worker-{uuid4()}"
    lease_token = str(uuid4())
    now = datetime.now(timezone.utc)
    with _connect() as connection:
        connection.execute(
            """
            INSERT INTO generation_worker_leases
                (job_id, worker_id, lease_token, acquired_at, expires_at)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (job_id, worker_id, lease_token, now, now + timedelta(minutes=1)),
        )

    with _connect() as connection:
        tx = PostgresPersistenceTransaction(connection)
        tx.assert_lease_owner(job_id, lease_token)
        tx.append_event(JobEvent(
            event_id=f"{job_id}:lease-checked", job_id=job_id,
            event_type=JobEventType.STARTED, occurred_at=datetime.now(timezone.utc),
            attempt_number=1,
        ))
        tx.commit()

    with _connect() as connection:
        assert connection.execute(
            "SELECT count(*) FROM job_events WHERE event_id = %s",
            (f"{job_id}:lease-checked",),
        ).fetchone()[0] == 1


def test_lease_expiring_after_initial_check_rolls_back_commit():
    job_id, _, _ = _prepare()
    worker_id = f"worker-{uuid4()}"
    lease_token = str(uuid4())
    now = datetime.now(timezone.utc)
    with _connect() as connection:
        connection.execute(
            """
            INSERT INTO generation_worker_leases
                (job_id, worker_id, lease_token, acquired_at, expires_at)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (job_id, worker_id, lease_token, now, now + timedelta(minutes=1)),
        )

    with _connect() as connection:
        tx = PostgresPersistenceTransaction(connection)
        tx.assert_lease_owner(job_id, lease_token)
        connection.execute(
            """
            UPDATE generation_worker_leases
            SET acquired_at = %s, expires_at = %s
            WHERE job_id = %s
            """,
            (
                datetime.now(timezone.utc) - timedelta(seconds=2),
                datetime.now(timezone.utc) - timedelta(seconds=1),
                job_id,
            ),
        )
        tx.append_event(JobEvent(
            event_id=f"{job_id}:must-rollback", job_id=job_id,
            event_type=JobEventType.STARTED, occurred_at=datetime.now(timezone.utc),
            attempt_number=1,
        ))
        with pytest.raises(LeaseOwnershipLost):
            tx.commit()

    with _connect() as connection:
        assert connection.execute(
            "SELECT count(*) FROM job_events WHERE event_id = %s",
            (f"{job_id}:must-rollback",),
        ).fetchone()[0] == 0
