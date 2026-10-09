from __future__ import annotations

from psycopg import Connection
from psycopg.types.json import Jsonb

from app.application.execution import LeaseOwnershipLost
from app.application.idempotency import (
    IdempotencyConflict,
    IdempotencyReservation,
    IdempotencyRepository,
    ReservationStatus,
    normalize_request_fingerprint,
)
from app.application.persistence import (
    ExecutionPersistenceTransaction,
    GenerationAttemptRepository,
    GenerationJobRepository,
    JobEventStore,
    SubmissionPersistenceTransaction,
)
from app.application.provider_operation_repository import ProviderOperationRepository
from app.application.work_intent import WorkIntentRepository
from app.domain.events import JobEvent
from app.domain.generation import (
    AttemptStatus,
    GenerationAttempt,
    GenerationJob,
    GenerationStatus,
    normalize_idempotency_key,
)
from app.infrastructure.postgres_provider_operation import PostgresProviderOperationRepository
from app.infrastructure.postgres_work_intent import PostgresWorkIntentRepository


class PostgresGenerationJobRepository(GenerationJobRepository):
    """Job repository bound to the transaction's shared connection."""

    def __init__(self, connection: Connection) -> None:
        self._connection = connection

    def add(self, job: GenerationJob) -> None:
        self._connection.execute(
            """
            INSERT INTO generation_jobs
                (job_id, capability, idempotency_key, status, attempt_count, failure_code)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                job.job_id, job.capability, job.idempotency_key, job.status.value,
                job.attempt_count, job.failure_code,
            ),
        )

    def get(self, job_id: str) -> GenerationJob | None:
        # Lock the aggregate row so read/modify/save is serialized inside this
        # transaction. The lock is held until the owner commits or rolls back.
        row = self._connection.execute(
            """
            SELECT job_id, capability, idempotency_key, status, attempt_count, failure_code
            FROM generation_jobs WHERE job_id = %s
            FOR UPDATE
            """,
            (job_id,),
        ).fetchone()
        if row is None:
            return None
        return GenerationJob(
            job_id=row[0],
            capability=row[1],
            idempotency_key=row[2],
            status=GenerationStatus(row[3]),
            attempt_count=row[4],
            failure_code=row[5],
        )

    def save(self, job: GenerationJob) -> None:
        updated = self._connection.execute(
            """
            UPDATE generation_jobs
            SET capability = %s, idempotency_key = %s, status = %s,
                attempt_count = %s, failure_code = %s,
                version = version + 1, updated_at = now()
            WHERE job_id = %s
            RETURNING job_id
            """,
            (
                job.capability, job.idempotency_key, job.status.value,
                job.attempt_count, job.failure_code, job.job_id,
            ),
        ).fetchone()
        if updated is None:
            raise KeyError(f"generation job not found: {job.job_id}")


class PostgresGenerationAttemptRepository(GenerationAttemptRepository):
    """Append-only attempt history stored as numbered immutable versions."""

    def __init__(self, connection: Connection) -> None:
        self._connection = connection

    def add(self, attempt: GenerationAttempt) -> None:
        if attempt.status is not AttemptStatus.RUNNING or attempt.completed_at is not None:
            raise ValueError("new generation attempt must be RUNNING")
        self._connection.execute(
            """
            INSERT INTO generation_attempt_versions
                (attempt_id, version, job_id, attempt_number, provider, started_at,
                 completed_at, status, provider_operation_id, failure_code)
            VALUES (%s, 1, %s, %s, %s, %s, NULL, 'RUNNING', %s, NULL)
            """,
            (
                attempt.attempt_id, attempt.job_id, attempt.attempt_number,
                attempt.provider, attempt.started_at, attempt.provider_operation_id,
            ),
        )

    def complete(self, attempt: GenerationAttempt) -> None:
        if attempt.status is AttemptStatus.RUNNING or attempt.completed_at is None:
            raise ValueError("completed attempt must be terminal")
        latest = self._connection.execute(
            """
            SELECT version, status FROM generation_attempt_versions
            WHERE attempt_id = %s ORDER BY version DESC LIMIT 1 FOR UPDATE
            """,
            (attempt.attempt_id,),
        ).fetchone()
        if latest is None:
            raise KeyError(f"attempt not found: {attempt.attempt_id}")
        if latest[1] != AttemptStatus.RUNNING.value:
            raise ValueError(f"attempt is already terminal: {attempt.attempt_id}")
        self._connection.execute(
            """
            INSERT INTO generation_attempt_versions
                (attempt_id, version, job_id, attempt_number, provider, started_at,
                 completed_at, status, provider_operation_id, failure_code)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                attempt.attempt_id, latest[0] + 1, attempt.job_id,
                attempt.attempt_number, attempt.provider, attempt.started_at,
                attempt.completed_at, attempt.status.value,
                attempt.provider_operation_id, attempt.failure_code,
            ),
        )

    def history(self, attempt_id: str) -> tuple[GenerationAttempt, ...]:
        rows = self._connection.execute(
            """
            SELECT attempt_id, job_id, attempt_number, provider, started_at,
                   completed_at, status, provider_operation_id, failure_code
            FROM generation_attempt_versions
            WHERE attempt_id = %s ORDER BY version
            """,
            (attempt_id,),
        ).fetchall()
        return tuple(
            GenerationAttempt(
                attempt_id=row[0], job_id=row[1], attempt_number=row[2],
                provider=row[3], started_at=row[4], completed_at=row[5],
                status=AttemptStatus(row[6]), provider_operation_id=row[7],
                failure_code=row[8],
            )
            for row in rows
        )


class PostgresJobEventStore(JobEventStore):
    """Append-only event store bound to the shared connection."""

    def __init__(self, connection: Connection) -> None:
        self._connection = connection

    def append(self, event: JobEvent) -> None:
        self._connection.execute(
            """
            INSERT INTO job_events
                (event_id, job_id, event_type, occurred_at, attempt_number,
                 failure_code, metadata)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                event.event_id, event.job_id, event.event_type.value,
                event.occurred_at, event.attempt_number, event.failure_code,
                Jsonb(dict(event.metadata)),
            ),
        )


class PostgresIdempotencyRepository(IdempotencyRepository):
    """Atomic idempotency reservation; must share the job transaction."""

    def __init__(self, connection: Connection, scope: str = "generation") -> None:
        if not scope.strip():
            raise ValueError("idempotency scope cannot be blank")
        self._connection = connection
        self._scope = scope.strip()

    def reserve(
        self, *, key: str, request_fingerprint: str, job_id: str
    ) -> IdempotencyReservation:
        normalized_key = normalize_idempotency_key(key)
        fingerprint = normalize_request_fingerprint(request_fingerprint)
        if not job_id.strip():
            raise ValueError("job_id cannot be blank")
        inserted = self._connection.execute(
            """
            INSERT INTO idempotency_reservations
                (scope, idempotency_key, request_fingerprint, job_id)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (scope, idempotency_key) DO NOTHING
            RETURNING scope
            """,
            (self._scope, normalized_key, fingerprint, job_id.strip()),
        ).fetchone()
        if inserted is not None:
            return IdempotencyReservation(
                normalized_key, fingerprint, job_id.strip(), ReservationStatus.CREATED
            )
        row = self._connection.execute(
            """
            SELECT request_fingerprint, job_id FROM idempotency_reservations
            WHERE scope = %s AND idempotency_key = %s
            FOR UPDATE
            """,
            (self._scope, normalized_key),
        ).fetchone()
        if row is None:
            raise RuntimeError("idempotency reservation disappeared during lookup")
        if row[0] != fingerprint:
            raise IdempotencyConflict(
                "idempotency key is already reserved for a different request"
            )
        return IdempotencyReservation(
            normalized_key, row[0], row[1], ReservationStatus.EXISTING
        )


class PostgresPersistenceTransaction(
    SubmissionPersistenceTransaction,
    ExecutionPersistenceTransaction,
):
    """Compose all PostgreSQL persistence ports over one caller-owned connection.

    The transaction object is the sole commit/rollback owner for all adapters
    it exposes. Do not give these repositories a separate connection or pool.
    The connection remains caller-owned and may be reused after commit/rollback.
    """

    def __init__(self, connection: Connection, *, idempotency_scope: str = "generation") -> None:
        self._connection = connection
        self.jobs = PostgresGenerationJobRepository(connection)
        self.attempts = PostgresGenerationAttemptRepository(connection)
        self.provider_operations: ProviderOperationRepository = (
            PostgresProviderOperationRepository(connection)
        )
        self.work_intents: WorkIntentRepository = PostgresWorkIntentRepository(connection)
        self.idempotency: IdempotencyRepository = PostgresIdempotencyRepository(
            connection, scope=idempotency_scope
        )
        self.events = PostgresJobEventStore(connection)
        self._lease_checks: set[tuple[str, str]] = set()

    def append_event(self, event: JobEvent) -> None:
        self.events.append(event)

    def assert_lease_owner(self, job_id: str, lease_token: str) -> None:
        if not lease_token.strip():
            raise LeaseOwnershipLost(f"lease ownership lost: {job_id}")
        row = self._connection.execute(
            """
            SELECT job_id
            FROM generation_worker_leases
            WHERE job_id = %s
              AND lease_token = %s::uuid
              AND expires_at > now()
            """,
            (job_id, lease_token),
        ).fetchone()
        if row is None:
            raise LeaseOwnershipLost(f"lease ownership lost: {job_id}")
        # Revalidate and lock only at commit time. Do not hold the queue-row lock
        # across a potentially slow external provider call.
        self._lease_checks.add((job_id, lease_token))

    def commit(self) -> None:
        try:
            for job_id, lease_token in sorted(self._lease_checks):
                row = self._connection.execute(
                    """
                    SELECT message_id
                    FROM generation_work_items
                    WHERE job_id = %s
                      AND state = 'CLAIMED'
                      AND claim_token = %s::uuid
                      AND claimed_until > now()
                    FOR UPDATE
                    """,
                    (job_id, lease_token),
                ).fetchone()
                if row is None:
                    raise LeaseOwnershipLost(f"lease ownership lost: {job_id}")
            self._connection.commit()
        except Exception:
            self._connection.rollback()
            self._lease_checks.clear()
            raise

    def rollback(self) -> None:
        self._connection.rollback()
