from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from threading import Event
from uuid import uuid4
import os
from pathlib import Path

import psycopg
import pytest

from app.application.ports import GenerationResult, ProviderOperationStatusResult
from app.application.provider_operation import ProviderOperation, ProviderOperationStatus
from app.application.provider_operation_lifecycle import (
    ProviderOperationLifecycle,
    ProviderOperationPersistenceUncertain,
)
from app.application.reconciliation import ProviderExecutionContract, ProviderOperationSafety
from app.infrastructure.postgres_persistence_factory import PostgresPersistenceTransactionFactory


MIGRATION = Path(__file__).parents[2] / "migrations" / "0001_postgresql_durable_work.sql"


class FixedStatusProvider:
    def __init__(self, provider_name, status, *, entered=None, release=None):
        self.provider_name = provider_name
        self.status = status
        self.entered = entered
        self.release = release

    def get_status(self, operation):
        if self.entered is not None:
            self.entered.set()
        if self.release is not None:
            assert self.release.wait(timeout=10)
        if self.status is ProviderOperationStatus.SUCCEEDED:
            return ProviderOperationStatusResult(
                operation=operation,
                status=self.status,
                result=GenerationResult(
                    provider=self.provider_name,
                    provider_operation_id=operation.operation_id,
                    artifact_refs=("artifact://terminal-race/result.mp4",),
                ),
            )
        return ProviderOperationStatusResult(operation=operation, status=self.status)

    def submit(self, request):
        raise AssertionError("submit is not expected")

    def cancel(self, operation):
        raise AssertionError("cancel is not expected")


def test_stale_running_poll_cannot_regress_concurrently_committed_terminal_result():
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        pytest.skip("DATABASE_URL is required for PostgreSQL integration tests")

    suffix = uuid4().hex
    job_id = f"terminal-race-job-{suffix}"
    provider_name = f"terminal-race-provider-{suffix}"
    operation_id = f"terminal-race-operation-{suffix}"
    idempotency_key = f"terminal-race-key-{suffix}"
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

    stale_observed = Event()
    release_stale = Event()
    factory = PostgresPersistenceTransactionFactory(lambda: psycopg.connect(database_url))
    contract = ProviderExecutionContract(provider_name, ProviderOperationSafety.IDEMPOTENT)

    def stale_poll():
        transaction = factory()
        lifecycle = ProviderOperationLifecycle(
            transaction,
            FixedStatusProvider(
                provider_name,
                ProviderOperationStatus.RUNNING,
                entered=stale_observed,
                release=release_stale,
            ),
            provider_name,
            contract,
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

    with ThreadPoolExecutor(max_workers=1) as executor:
        stale_future = executor.submit(stale_poll)
        assert stale_observed.wait(timeout=10)

        terminal_tx = factory()
        terminal_lifecycle = ProviderOperationLifecycle(
            terminal_tx,
            FixedStatusProvider(provider_name, ProviderOperationStatus.SUCCEEDED),
            provider_name,
            contract,
        )
        try:
            terminal = terminal_lifecycle.poll(
                operation,
                next_poll_due_at=now + timedelta(seconds=30),
                expected_poll_generation=0,
                job_id=job_id,
            )
            assert terminal.status is ProviderOperationStatus.SUCCEEDED
        finally:
            terminal_tx.close()

        release_stale.set()
        assert stale_future.result(timeout=10) is None

    with psycopg.connect(database_url) as connection:
        stored = connection.execute(
            """
            SELECT status, poll_generation, version, terminal_result
            FROM provider_operations WHERE provider = %s AND operation_id = %s
            """,
            (provider_name, operation_id),
        ).fetchone()
        intent_count = connection.execute(
            """
            SELECT count(*) FROM generation_work_items
            WHERE job_id = %s AND intent_kind = 'PROVIDER_POLL'
            """,
            (job_id,),
        ).fetchone()[0]

    assert stored[0:3] == ("SUCCEEDED", 0, 1)
    assert stored[3]["artifact_refs"] == ["artifact://terminal-race/result.mp4"]
    assert intent_count == 0
