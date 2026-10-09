from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from threading import Barrier
from uuid import uuid4
import os
from pathlib import Path

import psycopg

from app.application.ports import ProviderOperationStatusResult
from app.application.provider_operation import ProviderOperation, ProviderOperationStatus
from app.application.provider_operation_lifecycle import (
    ProviderOperationLifecycle,
    ProviderOperationPersistenceUncertain,
)
from app.application.reconciliation import ProviderExecutionContract, ProviderOperationSafety
from app.application.work_intent import WorkIntent
from app.domain.generation import GenerationJob
from app.infrastructure.postgres_persistence_factory import PostgresPersistenceTransactionFactory


MIGRATION = Path(__file__).parents[2] / "migrations" / "0001_postgresql_durable_work.sql"


class BarrierRunningProvider:
    def __init__(self, provider_name: str, barrier: Barrier) -> None:
        self.provider_name = provider_name
        self.barrier = barrier
        self.calls = 0

    def get_status(self, operation: ProviderOperation) -> ProviderOperationStatusResult:
        self.barrier.wait(timeout=10)
        self.calls += 1
        return ProviderOperationStatusResult(
            operation=operation,
            status=ProviderOperationStatus.RUNNING,
        )

    def submit(self, request):
        raise AssertionError("submit is not expected in this poll race")

    def cancel(self, operation):
        raise AssertionError("cancel is not expected in this poll race")


def test_concurrent_poll_observations_cannot_advance_same_generation_twice():
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        import pytest
        pytest.skip("DATABASE_URL is required for PostgreSQL integration tests")

    suffix = uuid4().hex
    job_id = f"poll-race-job-{suffix}"
    provider_name = f"poll-race-provider-{suffix}"
    operation_id = f"poll-race-operation-{suffix}"
    idempotency_key = f"poll-race-key-{suffix}"
    now = datetime.now(timezone.utc)
    operation = ProviderOperation(
        provider=provider_name,
        operation_id=operation_id,
        idempotency_key=idempotency_key,
        capability="video",
    )
    with psycopg.connect(database_url) as connection:
        connection.execute(MIGRATION.read_text(encoding="utf-8"))
        connection.execute(
            """
            INSERT INTO generation_jobs (job_id, capability, idempotency_key, status)
            VALUES (%s, 'video', %s, 'RUNNING')
            """,
            (job_id, idempotency_key),
        )
        from app.infrastructure.postgres_provider_operation import PostgresProviderOperationRepository
        PostgresProviderOperationRepository(connection).add(operation)

    barrier = Barrier(2)
    provider = BarrierRunningProvider(provider_name, barrier)
    transaction_factory = PostgresPersistenceTransactionFactory(
        lambda: psycopg.connect(database_url)
    )
    contract = ProviderExecutionContract(provider_name, ProviderOperationSafety.IDEMPOTENT)

    def poll_once():
        transaction = transaction_factory()
        lifecycle = ProviderOperationLifecycle(
            transaction=transaction,
            provider=provider,
            provider_name=provider_name,
            contract=contract,
        )
        try:
            return lifecycle.poll(
                operation,
                next_poll_due_at=now + timedelta(seconds=30),
                expected_poll_generation=0,
                job_id=job_id,
            )
        except ProviderOperationPersistenceUncertain:
            return None
        finally:
            transaction.close()

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = list(executor.map(lambda _: poll_once(), range(2)))

    assert provider.calls == 2
    assert sum(outcome is not None for outcome in outcomes) == 1

    with psycopg.connect(database_url) as connection:
        row = connection.execute(
            """
            SELECT status, poll_generation, version
            FROM provider_operations WHERE provider = %s AND operation_id = %s
            """,
            (provider_name, operation_id),
        ).fetchone()
        intents = connection.execute(
            """
            SELECT count(*) FROM generation_work_items
            WHERE job_id = %s AND intent_kind = 'PROVIDER_POLL'
              AND provider = %s AND operation_id = %s AND generation = 1
            """,
            (job_id, provider_name, operation_id),
        ).fetchone()[0]

    assert row == ("RUNNING", 1, 1)
    assert intents == 1
