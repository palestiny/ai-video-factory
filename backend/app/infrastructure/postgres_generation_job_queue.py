from __future__ import annotations

from datetime import datetime, timedelta
from typing import Callable
from uuid import uuid4

from psycopg import Connection

from app.application.worker import GenerationJobQueue, QueueMessage


class PostgresGenerationJobQueue(GenerationJobQueue):
    """PostgreSQL at-least-once queue with token-fenced delivery operations.

    Each public operation owns a short database transaction obtained from the
    supplied connection factory. Work-intent insertion that must be atomic with
    domain state must use PostgresWorkIntentRepository on the domain transaction's
    connection instead of calling enqueue/enqueue_after.
    """

    def __init__(
        self,
        connection_factory: Callable[[], Connection],
        *,
        max_delivery_attempts: int = 5,
        retry_base_delay: timedelta = timedelta(seconds=1),
        max_retry_delay: timedelta = timedelta(minutes=1),
    ) -> None:
        if max_delivery_attempts < 1:
            raise ValueError("max_delivery_attempts must be at least 1")
        if retry_base_delay < timedelta(0) or max_retry_delay < timedelta(0):
            raise ValueError("retry delays cannot be negative")
        self._connection_factory = connection_factory
        self._max_delivery_attempts = max_delivery_attempts
        self._retry_base_delay = retry_base_delay
        self._max_retry_delay = max_retry_delay

    def enqueue(self, job_id: str) -> QueueMessage:
        return self.enqueue_after(job_id, timedelta(0))

    def enqueue_after(self, job_id: str, delay: timedelta) -> QueueMessage:
        if not job_id.strip():
            raise ValueError("job_id cannot be blank")
        if delay < timedelta(0):
            raise ValueError("delay cannot be negative")
        message_id = uuid4()
        intent_key = f"job-execution:{message_id}"
        with self._connection_factory() as connection:
            connection.execute(
                """
                INSERT INTO generation_work_items
                    (message_id, job_id, intent_key, intent_kind, due_at)
                VALUES (%s, %s, %s, 'JOB_EXECUTION', now() + %s)
                """,
                (message_id, job_id, intent_key, delay),
            )
        return QueueMessage(str(message_id), job_id)

    def claim_next(
        self,
        worker_id: str,
        now: datetime,
        lease_duration: timedelta,
    ) -> QueueMessage | None:
        if not worker_id.strip():
            raise ValueError("worker_id cannot be blank")
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("now must be timezone-aware")
        if lease_duration <= timedelta(0):
            raise ValueError("lease_duration must be positive")
        token = uuid4()
        with self._connection_factory() as connection:
            # An expired final attempt must become visible as DEAD instead of
            # being reclaimed forever after repeated process crashes.
            connection.execute(
                """
                UPDATE generation_work_items
                SET state = 'DEAD',
                    claim_token = NULL, claimed_by = NULL, claimed_until = NULL,
                    last_error_code = 'MAX_DELIVERY_ATTEMPTS_EXCEEDED'
                WHERE state = 'CLAIMED'
                  AND claimed_until <= %s
                  AND delivery_attempt >= %s
                """,
                (now, self._max_delivery_attempts),
            )
            row = connection.execute(
                """
                WITH candidate AS (
                    SELECT message_id
                    FROM generation_work_items
                    WHERE due_at <= %s
                      AND (state = 'PENDING'
                           OR (state = 'CLAIMED' AND claimed_until <= %s
                               AND delivery_attempt < %s))
                    ORDER BY due_at, created_at, message_id
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                )
                UPDATE generation_work_items AS item
                SET state = 'CLAIMED',
                    delivery_attempt = CASE
                        WHEN item.state = 'CLAIMED' THEN item.delivery_attempt + 1
                        ELSE item.delivery_attempt
                    END,
                    claim_token = %s,
                    claimed_by = %s,
                    claimed_until = %s + %s,
                    last_error_code = CASE
                        WHEN item.state = 'CLAIMED' THEN 'LEASE_EXPIRED'
                        ELSE item.last_error_code
                    END
                FROM candidate
                WHERE item.message_id = candidate.message_id
                RETURNING item.message_id, item.job_id, item.delivery_attempt,
                          item.intent_key, item.intent_kind, item.provider,
                          item.operation_id, item.generation
                """,
                (now, now, self._max_delivery_attempts, token, worker_id, now, lease_duration),
            ).fetchone()
        if row is None:
            return None
        return QueueMessage(
            str(row[0]), row[1], row[2], str(token),
            row[3], row[4], row[5], row[6], row[7],
        )

    def ack(self, message: QueueMessage) -> None:
        token = self._required_token(message)
        with self._connection_factory() as connection:
            updated = connection.execute(
                """
                UPDATE generation_work_items
                SET state = 'ACKED', acked_at = now(),
                    claim_token = NULL, claimed_by = NULL, claimed_until = NULL
                WHERE message_id = %s AND state = 'CLAIMED' AND claim_token = %s
                  AND claimed_until > now()
                RETURNING message_id
                """,
                (message.message_id, token),
            ).fetchone()
            if updated is None:
                raise ValueError("stale or unowned queue delivery cannot be acknowledged")

    def release_or_requeue(self, message: QueueMessage) -> None:
        token = self._required_token(message)
        exponent = min(max(message.delivery_attempt - 1, 0), 30)
        delay_seconds = min(
            self._retry_base_delay.total_seconds() * (2 ** exponent),
            self._max_retry_delay.total_seconds(),
        )
        with self._connection_factory() as connection:
            updated = connection.execute(
                """
                UPDATE generation_work_items
                SET state = CASE
                        WHEN delivery_attempt >= %s THEN 'DEAD'
                        ELSE 'PENDING'
                    END,
                    due_at = CASE
                        WHEN delivery_attempt >= %s THEN due_at
                        ELSE now() + (%s * interval '1 second')
                    END,
                    delivery_attempt = CASE
                        WHEN delivery_attempt >= %s THEN delivery_attempt
                        ELSE delivery_attempt + 1
                    END,
                    claim_token = NULL, claimed_by = NULL, claimed_until = NULL,
                    last_error_code = CASE
                        WHEN delivery_attempt >= %s THEN 'MAX_DELIVERY_ATTEMPTS_EXCEEDED'
                        ELSE 'DELIVERY_RELEASED'
                    END
                WHERE message_id = %s AND state = 'CLAIMED' AND claim_token = %s
                  AND claimed_until > now()
                RETURNING message_id
                """,
                (
                    self._max_delivery_attempts,
                    self._max_delivery_attempts,
                    delay_seconds,
                    self._max_delivery_attempts,
                    self._max_delivery_attempts,
                    message.message_id,
                    token,
                ),
            ).fetchone()
            if updated is None:
                raise ValueError("stale or unowned queue delivery cannot be released")

    @staticmethod
    def _required_token(message: QueueMessage) -> str:
        token = message.claim_token
        if not token:
            raise ValueError("queue delivery has no claim token")
        return token
