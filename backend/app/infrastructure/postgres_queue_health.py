from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from psycopg import Connection


@dataclass(frozen=True)
class QueueHealthSnapshot:
    """Read-only operational snapshot of durable queue pressure and recovery state."""

    pending_count: int
    due_pending_count: int
    active_claim_count: int
    expired_claim_count: int
    dead_count: int
    oldest_unresolved_age_seconds: float | None
    oldest_due_pending_age_seconds: float | None
    max_delivery_attempt: int


class PostgresQueueHealthReader:
    """Read queue health without mutating work items or acquiring their row locks."""

    def __init__(self, connection_factory: Callable[[], Connection]) -> None:
        self._connection_factory = connection_factory

    def snapshot(self) -> QueueHealthSnapshot:
        with self._connection_factory() as connection:
            row = connection.execute(
                """
                WITH clock AS (
                    SELECT statement_timestamp() AS observed_at
                )
                SELECT
                    count(*) FILTER (WHERE item.state = 'PENDING'),
                    count(*) FILTER (
                        WHERE item.state = 'PENDING' AND item.due_at <= clock.observed_at
                    ),
                    count(*) FILTER (
                        WHERE item.state = 'CLAIMED' AND item.claimed_until > clock.observed_at
                    ),
                    count(*) FILTER (
                        WHERE item.state = 'CLAIMED' AND item.claimed_until <= clock.observed_at
                    ),
                    count(*) FILTER (WHERE item.state = 'DEAD'),
                    max(
                        extract(epoch FROM clock.observed_at - item.created_at)
                    ) FILTER (WHERE item.state IN ('PENDING', 'CLAIMED', 'DEAD'))::double precision,
                    max(
                        extract(epoch FROM clock.observed_at - item.due_at)
                    ) FILTER (
                        WHERE item.state = 'PENDING' AND item.due_at <= clock.observed_at
                    )::double precision,
                    coalesce(max(item.delivery_attempt), 0)
                FROM generation_work_items AS item
                CROSS JOIN clock
                """
            ).fetchone()

        return QueueHealthSnapshot(
            pending_count=row[0],
            due_pending_count=row[1],
            active_claim_count=row[2],
            expired_claim_count=row[3],
            dead_count=row[4],
            oldest_unresolved_age_seconds=row[5],
            oldest_due_pending_age_seconds=row[6],
            max_delivery_attempt=row[7],
        )
