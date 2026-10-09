from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Barrier, Lock
import os
from uuid import uuid4

import psycopg
import pytest

from app.application.ports import GenerationRequest
from app.application.provider_operation import (
    ProviderOperation,
    ProviderOperationStatus,
    ProviderOperationStatusResult,
)
from app.application.provider_operation_lifecycle import (
    ProviderOperationLifecycle,
    ProviderOperationPersistenceUncertain,
)
from app.application.reconciliation import ProviderExecutionContract, ProviderOperationSafety
from app.domain.generation import GenerationJob
from app.infrastructure.postgres_persistence import PostgresPersistenceTransaction


MIGRATION = Path(__file__).parents[2] / "migrations" / "0001_postgresql_durable_work.sql"


class ConcurrentPollProvider:
    provider_name = "concurrent-poll-provider"

    def __init__(self, barrier: Barrier) -> None:
        self._barrier = barrier
        self._lock = Lock()
        self.poll_count = 0

    def get_status(self, operation: ProviderOperation) -> ProviderOperationStatusResult:
        with self._lock:
            self.poll_count += 1
        # Force both independent transactions to observe the same pre-update
        # operation version before either can attempt its compare-and-swap write.
        self._barrier.wait(timeout=10)
        return ProviderOperationStatusResult(
            operation=operation,
            status=ProviderOperationStatus.RUNNING,
        )

    def submit(self, request: GenerationRequest) -> ProviderOperation:
        raise AssertionError("this test exercises poll scheduling, not submission")

    def cancel(self, operation: ProviderOperation):
        raise AssertionError("cancellation is not part of this test")


def test_concurrent_same_generation_polls_commit_only_one_next_poll_intent():
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        pytest.skip("DATABASE_URL is required for PostgreSQL integration tests")

    suffix = uuid4().hex
    job_id = f"concurrent-poll-job-{suffix}"
    provider_name = "concurrent-poll-provider"
    operation_id = f"operation-{suffix}"
    idempotency_key = f"idempotency-{suffix}"

    with psycopg.connect(database_url) as connection:
        connection.execute(MIGRATION.read_text(encoding="utf-8"))
        connection.execute(
            """
            INSERT INTO generation_jobs (job_id, capability, idempotency_key, status)
            VALUES (%s, 'video', %s, 'RUNNING')
            """,
            (job_id, idempotency_key),
        )
        PostgresPersistenceTransaction(connection).provider_operations.add(
            ProviderOperation(
                provider=provider_name,
                operation_id=operation_id,
                idempotency_key=idempotency_key,
                capability="video",
            )
        )

    barrier = Barrier(2)
    provider = ConcurrentPollProvider(barrier)
    contract = ProviderExecutionContract(
        provider=provider_name,
        operation_safety=ProviderOperationSafety.IDEMPOTENT,
    )
    due_at = datetime.now(timezone.utc) + timedelta(seconds=30)

    def poll_once():
        with psycopg.connect(database_url) as connection:
            tx = PostgresPersistenceTransaction(connection)
            current = tx.provider_operations.get(provider_name, operation_id)
            assert current is not None
            lifecycle = ProviderOperationLifecycle(tx, provider, provider_name, contract)
            try:
                lifecycle.poll(
                    current,
                    next_poll_due_at=due_at,
                    expected_poll_generation=0,
                    job_id=job_id,
                )
                return "committed"
            except ProviderOperationPersistenceUncertain:
                return "fenced"

    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(lambda _: poll_once(), range(2)))

    assert sorted(outcomes) == ["committed", "fenced"]
    assert provider.poll_count == 2

    with psycopg.connect(database_url) as connection:
        operation = connection.execute(
            """
            SELECT status, poll_generation, version
            FROM provider_operations
            WHERE provider = %s AND operation_id = %s
            """,
            (provider_name, operation_id),
        ).fetchone()
        intents = connection.execute(
            """
            SELECT count(*), min(generation), max(generation)
            FROM generation_work_items
            WHERE job_id = %s AND intent_kind = 'PROVIDER_POLL'
              AND provider = %s AND operation_id = %s
            """,
            (job_id, provider_name, operation_id),
        ).fetchone()

    assert operation == ("RUNNING", 1, 1)
    assert intents == (1, 1, 1)
