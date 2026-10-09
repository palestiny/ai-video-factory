from __future__ import annotations

import os
from datetime import timedelta

import psycopg

from app.application.async_worker_execution import (
    AsyncProviderContractResolver,
    AsyncProviderResolver,
    ExecuteAsyncProviderDelivery,
)
from app.infrastructure.postgres_generation_job_queue import PostgresGenerationJobQueue
from app.infrastructure.postgres_persistence_factory import PostgresPersistenceTransactionFactory
from app.infrastructure.postgres_worker_lease import PostgresWorkerLeaseRepository


def build_postgres_async_worker(
    *,
    providers: AsyncProviderResolver,
    contracts: AsyncProviderContractResolver,
    database_url: str | None = None,
    lease_duration: timedelta = timedelta(minutes=2),
    poll_delay: timedelta = timedelta(seconds=5),
) -> ExecuteAsyncProviderDelivery:
    """Build the PostgreSQL-backed worker without choosing a hosting vendor.

    The connection string is explicit when supplied; otherwise DATABASE_URL is
    required. Queue operations and worker leases use short-lived connections,
    while each delivery receives a managed transaction connection that is
    closed by the worker's delivery-finally cleanup.
    """
    resolved_url = (database_url or os.environ.get("DATABASE_URL", "")).strip()
    if not resolved_url:
        raise RuntimeError("DATABASE_URL must be configured for the PostgreSQL worker")
    if lease_duration <= timedelta(0):
        raise ValueError("lease_duration must be positive")
    if poll_delay < timedelta(0):
        raise ValueError("poll_delay cannot be negative")

    def connect() -> psycopg.Connection:
        return psycopg.connect(resolved_url)

    return ExecuteAsyncProviderDelivery(
        queue=PostgresGenerationJobQueue(connect),
        leases=PostgresWorkerLeaseRepository(connect),
        transaction_factory=PostgresPersistenceTransactionFactory(connect),
        providers=providers,
        contracts=contracts,
        lease_duration=lease_duration,
        poll_delay=poll_delay,
    )
