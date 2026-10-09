from dataclasses import replace
from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest

from app.application.idempotency import ReservationStatus
from app.application.provider_operation import ProviderOperation, ProviderOperationStatus
from app.domain.events import JobEvent, JobEventType
from app.domain.generation import GenerationAttempt, GenerationJob, GenerationStatus
from app.application.execution import LeaseOwnershipLost
from app.application.work_intent import WorkIntent
from app.infrastructure.postgres_persistence import PostgresPersistenceTransaction
from app.infrastructure.postgres_provider_operation import PostgresProviderOperationRepository


MIGRATION = Path(__file__).parents[2] / "migrations" / "0001_postgresql_durable_work.sql"


def _database() -> str:
    value = os.environ.get("DATABASE_URL")
    if not value:
        pytest.skip("DATABASE_URL is required for PostgreSQL integration tests")
    return value


def _prepare() -> tuple[str, str, str, str]:
    job_id = "tx-job-" + uuid4().hex
    provider = "tx-provider-" + uuid4().hex
    operation_id = "tx-operation-" + uuid4().hex
    idempotency_key = "tx-key-" + uuid4().hex
    with psycopg.connect(_database()) as connection:
        connection.execute(MIGRATION.read_text(encoding="utf-8"))
        connection.execute(
            """
            INSERT INTO generation_jobs (job_id, capability, idempotency_key, status)
            VALUES (%s, 'video', %s, 'RUNNING')
            """,
            (job_id, job_id + "-idempotency"),
        )
        PostgresProviderOperationRepository(connection).add(
            ProviderOperation(
                provider=provider,
                operation_id=operation_id,
                idempotency_key=idempotency_key,
                capability="video",
            )
        )
    return job_id, provider, operation_id, idempotency_key


def _intent(job_id: str, provider: str, operation_id: str) -> WorkIntent:
    return WorkIntent.provider_poll(
        job_id=job_id,
        provider=provider,
        operation_id=operation_id,
        generation=1,
        due_at=datetime.now(timezone.utc) + timedelta(seconds=45),
    )


def test_composed_transaction_commits_operation_intent_attempt_and_event_together():
    job_id, provider, operation_id, _ = _prepare()
    intent = _intent(job_id, provider, operation_id)
    attempt_id = job_id + ":attempt-1"
    event_id = job_id + ":running"
    with psycopg.connect(_database()) as connection:
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
        transaction.attempts.add(
            GenerationAttempt.started(
                attempt_id=attempt_id,
                job_id=job_id,
                attempt_number=1,
                provider=provider,
                started_at=datetime.now(timezone.utc),
            )
        )
        transaction.append_event(
            JobEvent(
                event_id=event_id,
                job_id=job_id,
                event_type=JobEventType.STARTED,
                occurred_at=datetime.now(timezone.utc),
                attempt_number=1,
            )
        )
        transaction.commit()

    with psycopg.connect(_database()) as connection:
        transaction = PostgresPersistenceTransaction(connection)
        stored = transaction.provider_operations.get(provider, operation_id)
        assert stored is not None
        assert stored.status is ProviderOperationStatus.RUNNING
        assert stored.poll_generation == 1 and stored.version == 1
        assert transaction.work_intents.get(intent.intent_key) == intent
        assert len(transaction.attempts.history(attempt_id)) == 1
        event = connection.execute(
            "SELECT event_type FROM job_events WHERE event_id = %s", (event_id,)
        ).fetchone()
        assert event == ("STARTED",)


def test_composed_transaction_rollback_removes_all_staged_writes():
    job_id, provider, operation_id, _ = _prepare()
    intent = _intent(job_id, provider, operation_id)
    attempt_id = job_id + ":attempt-rollback"
    event_id = job_id + ":rollback-event"
    with psycopg.connect(_database()) as connection:
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
        transaction.work_intents.add_if_absent(intent)
        transaction.attempts.add(
            GenerationAttempt.started(
                attempt_id=attempt_id,
                job_id=job_id,
                attempt_number=1,
                provider=provider,
                started_at=datetime.now(timezone.utc),
            )
        )
        transaction.append_event(
            JobEvent(
                event_id=event_id,
                job_id=job_id,
                event_type=JobEventType.STARTED,
                occurred_at=datetime.now(timezone.utc),
                attempt_number=1,
            )
        )
        transaction.rollback()

    with psycopg.connect(_database()) as connection:
        transaction = PostgresPersistenceTransaction(connection)
        operation = transaction.provider_operations.get(provider, operation_id)
        assert operation is not None
        assert operation.status is ProviderOperationStatus.SUBMITTED
        assert operation.poll_generation == 0 and operation.version == 0
        assert transaction.work_intents.get(intent.intent_key) is None
        assert transaction.attempts.history(attempt_id) == ()
        assert connection.execute(
            "SELECT 1 FROM job_events WHERE event_id = %s", (event_id,)
        ).fetchone() is None


def test_transaction_validates_active_lease_token_and_expiry():
    job_id, _, _, _ = _prepare()
    message_id = uuid4()
    token = uuid4()
    with psycopg.connect(_database()) as connection:
        connection.execute(
            """
            INSERT INTO generation_worker_leases
                (job_id, worker_id, lease_token, acquired_at, expires_at)
            VALUES (%s, 'transaction-test-worker', %s, now(),
                    now() + interval '2 minutes')
            """,
            (job_id, token),
        )
        transaction = PostgresPersistenceTransaction(connection)
        transaction.assert_lease_owner(job_id, str(token))
        with pytest.raises(LeaseOwnershipLost):
            transaction.assert_lease_owner(job_id, str(uuid4()))
        connection.execute(
            "UPDATE generation_worker_leases SET acquired_at = now() - interval '5 minutes', expires_at = now() - interval '1 second' WHERE job_id = %s",
            (job_id,),
        )
        with pytest.raises(LeaseOwnershipLost):
            transaction.commit()


def test_idempotency_reservation_can_precede_job_insert_in_same_transaction():
    job_id = "tx-submission-" + uuid4().hex
    key = "request/" + uuid4().hex
    with psycopg.connect(_database()) as connection:
        transaction = PostgresPersistenceTransaction(connection)
        reservation = transaction.idempotency.reserve(
            key=key,
            request_fingerprint="fingerprint-1",
            job_id=job_id,
        )
        assert reservation.status is ReservationStatus.CREATED
        transaction.jobs.add(
            GenerationJob.create(
                job_id=job_id,
                capability="video",
                idempotency_key=key,
            )
        )
        transaction.append_event(
            JobEvent(
                event_id=job_id + ":created",
                job_id=job_id,
                event_type=JobEventType.CREATED,
                occurred_at=datetime.now(timezone.utc),
            )
        )
        transaction.commit()

    with psycopg.connect(_database()) as connection:
        transaction = PostgresPersistenceTransaction(connection)
        existing = transaction.idempotency.reserve(
            key=key,
            request_fingerprint="fingerprint-1",
            job_id="should-not-be-used",
        )
        assert existing.status is ReservationStatus.EXISTING
        assert existing.job_id == job_id
        assert transaction.jobs.get(job_id) is not None
