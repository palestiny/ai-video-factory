from datetime import timedelta

import pytest

from app.application.async_worker_execution import ExecuteAsyncProviderDelivery
from app.infrastructure.postgres_worker_factory import build_postgres_async_worker


class Providers:
    def resolve(self, capability: str):
        raise AssertionError("provider resolution must be deferred until delivery")


class Contracts:
    def resolve_contract(self, provider_name: str):
        raise AssertionError("contract resolution must be deferred until delivery")


def test_worker_factory_requires_database_url(monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    with pytest.raises(RuntimeError, match="DATABASE_URL must be configured"):
        build_postgres_async_worker(providers=Providers(), contracts=Contracts())


def test_worker_factory_wires_postgres_components_without_opening_connections():
    worker = build_postgres_async_worker(
        database_url="postgresql://unused-for-construction",
        providers=Providers(),
        contracts=Contracts(),
        lease_duration=timedelta(minutes=3),
        poll_delay=timedelta(seconds=7),
    )
    assert isinstance(worker, ExecuteAsyncProviderDelivery)


@pytest.mark.parametrize(
    ("lease_duration", "poll_delay", "message"),
    [
        (timedelta(0), timedelta(seconds=1), "lease_duration must be positive"),
        (timedelta(seconds=1), timedelta(seconds=-1), "poll_delay cannot be negative"),
    ],
)
def test_worker_factory_rejects_invalid_durations(lease_duration, poll_delay, message):
    with pytest.raises(ValueError, match=message):
        build_postgres_async_worker(
            database_url="postgresql://unused-for-construction",
            providers=Providers(),
            contracts=Contracts(),
            lease_duration=lease_duration,
            poll_delay=poll_delay,
        )
