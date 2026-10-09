from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest

from app.application.async_worker_execution import ExecuteAsyncProviderDelivery
from app.application.ports import GenerationRequest, GenerationResult
from app.application.provider_operation import (
    ProviderCancellationResult,
    ProviderOperation,
    ProviderOperationStatus,
    ProviderOperationStatusResult,
)
from app.application.reconciliation import ProviderExecutionContract, ProviderOperationSafety
from app.application.worker import QueueMessage
from app.application.worker_execution import WorkerDeliveryStatus
from app.domain.generation import GenerationJob, GenerationStatus
from app.infrastructure.postgres_generation_job_queue import PostgresGenerationJobQueue
from app.infrastructure.postgres_persistence_factory import PostgresPersistenceTransactionFactory
from app.infrastructure.postgres_worker_lease import PostgresWorkerLeaseRepository


MIGRATION = Path(__file__).parents[2] / "migrations" / "0001_postgresql_durable_work.sql"


class InjectedProcessCrash(BaseException):
    """Bypass the worker's ordinary exception handler like abrupt process death."""


class CrashBeforeFirstAckQueue:
    def __init__(self, delegate):
        self.delegate = delegate
        self.crash_once = True

    def __getattr__(self, name):
        return getattr(self.delegate, name)

    def ack(self, message: QueueMessage) -> None:
        if self.crash_once:
            self.crash_once = False
            raise InjectedProcessCrash()
        self.delegate.ack(message)


class RestartProvider:
    provider_name = "restart-provider"

    def __init__(self):
        self.submissions = 0
        self.polls = 0

    def submit(self, request: GenerationRequest) -> ProviderOperation:
        self.submissions += 1
        return ProviderOperation(
            provider=self.provider_name,
            operation_id=f"operation-{request.job_id}",
            idempotency_key=request.idempotency_key,
            capability=request.capability,
        )

    def get_status(self, operation: ProviderOperation) -> ProviderOperationStatusResult:
        self.polls += 1
        if self.polls == 1:
            return ProviderOperationStatusResult(
                operation=operation,
                status=ProviderOperationStatus.RUNNING,
            )
        result = GenerationResult(
            provider=self.provider_name,
            provider_operation_id=operation.operation_id,
            artifact_refs=("artifact://recovered/final.mp4",),
        )
        return ProviderOperationStatusResult(
            operation=operation,
            status=ProviderOperationStatus.SUCCEEDED,
            result=result,
        )

    def cancel(self, operation: ProviderOperation) -> ProviderCancellationResult:
        raise AssertionError("cancel is not expected")


class ProviderResolver:
    def __init__(self, provider):
        self.provider = provider

    def resolve(self, capability: str):
        assert capability == "video"
        return self.provider


class ContractResolver:
    def resolve_contract(self, provider_name: str):
        return ProviderExecutionContract(provider_name, ProviderOperationSafety.IDEMPOTENT)


def test_async_worker_recovers_after_commit_before_ack_using_postgres():
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        pytest.skip("DATABASE_URL is required for PostgreSQL integration tests")

    job_id = "worker-restart-" + uuid4().hex
    now = datetime.now(timezone.utc)
    with psycopg.connect(database_url) as connection:
        connection.execute(MIGRATION.read_text(encoding="utf-8"))
        connection.execute("DELETE FROM generation_work_items WHERE state IN ('PENDING', 'CLAIMED')")
        connection.execute(
            """
            INSERT INTO generation_jobs (job_id, capability, idempotency_key, status)
            VALUES (%s, 'video', %s, 'QUEUED')
            """,
            (job_id, job_id + "-key"),
        )

    base_queue = PostgresGenerationJobQueue(lambda: psycopg.connect(database_url))
    crash_queue = CrashBeforeFirstAckQueue(base_queue)
    leases = PostgresWorkerLeaseRepository(lambda: psycopg.connect(database_url))
    provider = RestartProvider()

    transaction_factory = PostgresPersistenceTransactionFactory(
        lambda: psycopg.connect(database_url)
    )

    def build_worker(queue):
        return ExecuteAsyncProviderDelivery(
            queue=queue,
            leases=leases,
            transaction_factory=transaction_factory,
            providers=ProviderResolver(provider),
            contracts=ContractResolver(),
            lease_duration=timedelta(minutes=2),
            poll_delay=timedelta(seconds=5),
        )

    base_queue.enqueue(job_id)
    now = datetime.now(timezone.utc) + timedelta(seconds=1)
    first_delivery = base_queue.claim_next("worker-before-crash", now, timedelta(seconds=30))
    assert first_delivery is not None

    with pytest.raises(InjectedProcessCrash):
        build_worker(crash_queue).handle(
            first_delivery, "worker-before-crash", now, {"prompt": "recoverable job"}
        )

    # The provider operation and next poll intent have committed; only the source
    # ACK is missing. Model the crashed process's queue lease expiring.
    with psycopg.connect(database_url) as connection:
        connection.execute(
            """
            UPDATE generation_work_items
            SET claimed_until = clock_timestamp() - interval '1 second'
            WHERE message_id = %s
            """,
            (first_delivery.message_id,),
        )
        state = connection.execute(
            """
            SELECT status, poll_generation FROM provider_operations
            WHERE provider = %s AND idempotency_key = %s
            """,
            (provider.provider_name, job_id + "-key"),
        ).fetchone()
        poll_intents = connection.execute(
            """
            SELECT count(*) FROM generation_work_items
            WHERE job_id = %s AND intent_kind = 'PROVIDER_POLL' AND generation = 1
            """,
            (job_id,),
        ).fetchone()[0]
    assert state == ("RUNNING", 1)
    assert poll_intents == 1
    assert provider.submissions == 1
    assert provider.polls == 1

    # Fresh queue/worker objects have no in-memory recovery state.
    restarted_queue = PostgresGenerationJobQueue(lambda: psycopg.connect(database_url))
    restarted_worker = build_worker(restarted_queue)
    recovered_source = restarted_queue.claim_next(
        "worker-after-restart", now + timedelta(minutes=1), timedelta(minutes=30)
    )
    assert recovered_source is not None
    assert recovered_source.message_id == first_delivery.message_id
    assert recovered_source.claim_token != first_delivery.claim_token

    result = restarted_worker.handle(
        recovered_source, "worker-after-restart", now + timedelta(minutes=1), {}
    )
    assert result.status is WorkerDeliveryStatus.ACKED
    # The original JOB_EXECUTION delivery has generation 0; the committed
    # generation 1 operation makes it stale, so restart does not poll twice.
    assert provider.submissions == 1
    assert provider.polls == 1

    due_poll = restarted_queue.claim_next(
        "worker-after-restart",
        now + timedelta(minutes=1, seconds=6),
        timedelta(minutes=2),
    )
    assert due_poll is not None
    assert due_poll.intent_kind == "PROVIDER_POLL"
    assert due_poll.generation == 1

    terminal = restarted_worker.handle(
        due_poll, "worker-after-restart", now + timedelta(minutes=1, seconds=6), {}
    )
    assert terminal.status is WorkerDeliveryStatus.ACKED
    assert provider.submissions == 1
    assert provider.polls == 2

    # A duplicate/stale execution delivery after terminal success must be
    # acknowledged without polling the provider or scheduling another intent.
    restarted_queue.enqueue(job_id)
    stale_delivery = restarted_queue.claim_next(
        "worker-after-restart",
        now + timedelta(minutes=2),
        timedelta(minutes=2),
    )
    assert stale_delivery is not None
    stale_result = restarted_worker.handle(
        stale_delivery, "worker-after-restart", now + timedelta(minutes=2), {}
    )
    assert stale_result.status is WorkerDeliveryStatus.ACKED
    assert provider.submissions == 1
    assert provider.polls == 2

    with psycopg.connect(database_url) as connection:
        job = connection.execute(
            "SELECT status FROM generation_jobs WHERE job_id = %s", (job_id,)
        ).fetchone()
        operation = connection.execute(
            """
            SELECT status, poll_generation FROM provider_operations
            WHERE provider = %s AND idempotency_key = %s
            """,
            (provider.provider_name, job_id + "-key"),
        ).fetchone()
        work_states = connection.execute(
            "SELECT intent_kind, state FROM generation_work_items WHERE job_id = %s ORDER BY created_at, message_id",
            (job_id,),
        ).fetchall()
        attempts = connection.execute(
            """
            SELECT status FROM generation_attempt_versions
            WHERE job_id = %s ORDER BY attempt_id, version
            """,
            (job_id,),
        ).fetchall()

    assert job == ("SUCCEEDED",)
    assert operation == ("SUCCEEDED", 1)
    assert ("JOB_EXECUTION", "ACKED") in work_states
    assert ("PROVIDER_POLL", "ACKED") in work_states
    assert [row[0] for row in attempts] == ["RUNNING", "SUCCEEDED"]
