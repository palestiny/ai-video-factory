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


def test_transaction_validates_active_worker_lease_token_and_expiry():
    job_id, _, _, _ = _prepare()
    token = uuid4()
    with psycopg.connect(_database()) as connection:
        connection.execute(
            """
            INSERT INTO generation_worker_leases
                (job_id, worker_id, lease_token, acquired_at, expires_at)
            VALUES (%s, 'transaction-test-worker', %s, clock_timestamp(),
                    clock_timestamp() + interval '2 minutes')
            """,
            (job_id, token),
        )

    with psycopg.connect(_database()) as connection:
        transaction = PostgresPersistenceTransaction(connection)
        transaction.assert_lease_owner(job_id, str(token))
        with pytest.raises(LeaseOwnershipLost):
            transaction.assert_lease_owner(job_id, str(uuid4()))
        transaction.rollback()

    with psycopg.connect(_database()) as connection:
        connection.execute(
            """
            UPDATE generation_worker_leases
            SET acquired_at = clock_timestamp() - interval '2 seconds',
                expires_at = clock_timestamp() - interval '1 second'
            WHERE job_id = %s
            """,
            (job_id,),
        )

    with psycopg.connect(_database()) as connection:
        transaction = PostgresPersistenceTransaction(connection)
        with pytest.raises(LeaseOwnershipLost):
            transaction.assert_lease_owner(job_id, str(token))
        transaction.rollback()


def test_transaction_rechecks_worker_lease_before_committing_writes():
    job_id, provider, operation_id, _ = _prepare()
    token = uuid4()
    with psycopg.connect(_database()) as connection:
        connection.execute(
            """
            INSERT INTO generation_worker_leases
                (job_id, worker_id, lease_token, acquired_at, expires_at)
            VALUES (%s, 'transaction-test-worker', %s, clock_timestamp(),
                    clock_timestamp() + interval '2 minutes')
            """,
            (job_id, token),
        )

    with psycopg.connect(_database()) as connection:
        transaction = PostgresPersistenceTransaction(connection)
        transaction.assert_lease_owner(job_id, str(token))
        operation = transaction.provider_operations.get(provider, operation_id)
        assert operation is not None
        transaction.provider_operations.save(
            replace(operation, status=ProviderOperationStatus.RUNNING, version=1)
        )

        # The worker's lease expires while provider work is in progress.
        with psycopg.connect(_database()) as other:
            other.execute(
                """
                UPDATE generation_worker_leases
            SET acquired_at = clock_timestamp() - interval '2 seconds',
                expires_at = clock_timestamp() - interval '1 second'
            WHERE job_id = %s
            """,
            (job_id,),
            )

        with pytest.raises(LeaseOwnershipLost):
            transaction.commit()

    with psycopg.connect(_database()) as connection:
        stored = connection.execute(
            "SELECT status, version FROM provider_operations WHERE provider = %s AND operation_id = %s",
            (provider, operation_id),
        ).fetchone()
    assert stored == ("SUBMITTED", 0)


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


def test_concurrent_idempotency_reservations_resolve_to_one_durable_job():
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier

    database_url = _database()
    barrier = Barrier(2)
    key = "tx-concurrent-key-" + uuid4().hex
    scope = "concurrent-reservation-" + uuid4().hex
    candidate_ids = [f"tx-concurrent-job-{uuid4().hex}" for _ in range(2)]

    def reserve(candidate_job_id: str):
        with psycopg.connect(database_url) as connection:
            transaction = PostgresPersistenceTransaction(
                connection, idempotency_scope=scope
            )
            barrier.wait(timeout=10)
            reservation = transaction.idempotency.reserve(
                key=key,
                request_fingerprint="same-canonical-request",
                job_id=candidate_job_id,
            )
            if reservation.status is ReservationStatus.CREATED:
                transaction.jobs.add(
                    GenerationJob.create(
                        candidate_job_id, "video", key
                    )
                )
            transaction.commit()
            return reservation.status, reservation.job_id

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(reserve, candidate_ids))

    statuses = [status for status, _ in results]
    assert sorted(status.value for status in statuses) == ["CREATED", "EXISTING"]
    assert len({job_id for _, job_id in results}) == 1

    with psycopg.connect(database_url) as connection:
        persisted = connection.execute(
            """
            SELECT job_id FROM idempotency_reservations
            WHERE scope = %s AND idempotency_key = %s
            """,
            (scope, key),
        ).fetchone()
        assert persisted == (results[0][1],)
        assert connection.execute(
            "SELECT count(*) FROM generation_jobs WHERE job_id = ANY(%s)",
            (candidate_ids,),
        ).fetchone() == (1,)
