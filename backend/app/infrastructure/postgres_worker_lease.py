from datetime import datetime, timedelta
from uuid import uuid4

from app.application.worker import WorkerLease, WorkerLeaseRepository


class PostgresWorkerLeaseRepository(WorkerLeaseRepository):
    """PostgreSQL-backed per-job leases with fresh-token fencing."""

    def __init__(self, connection_factory):
        self._connection_factory = connection_factory

    def claim(self, job_id: str, worker_id: str, now: datetime, lease_duration: timedelta) -> WorkerLease | None:
        self._validate(job_id, worker_id, now, lease_duration)
        token = uuid4()
        with self._connection_factory() as connection:
            row = connection.execute(
                """
                INSERT INTO generation_worker_leases
                    (job_id, worker_id, lease_token, acquired_at, expires_at)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (job_id) DO UPDATE
                SET worker_id = EXCLUDED.worker_id,
                    lease_token = EXCLUDED.lease_token,
                    acquired_at = EXCLUDED.acquired_at,
                    expires_at = EXCLUDED.expires_at
                WHERE generation_worker_leases.expires_at <= EXCLUDED.acquired_at
                RETURNING job_id, worker_id, lease_token, acquired_at, expires_at
                """,
                (job_id, worker_id, token, now, now + lease_duration),
            ).fetchone()
        return self._lease(row) if row else None

    def renew(self, job_id: str, lease_token: str, now: datetime, lease_duration: timedelta) -> WorkerLease | None:
        self._validate(job_id, "renew", now, lease_duration)
        with self._connection_factory() as connection:
            row = connection.execute(
                """
                UPDATE generation_worker_leases SET expires_at = %s + %s
                WHERE job_id = %s AND lease_token = %s::uuid AND expires_at > %s
                RETURNING job_id, worker_id, lease_token, acquired_at, expires_at
                """,
                (now, lease_duration, job_id, lease_token, now),
            ).fetchone()
        return self._lease(row) if row else None

    def release(self, job_id: str, lease_token: str) -> bool:
        with self._connection_factory() as connection:
            row = connection.execute(
                "DELETE FROM generation_worker_leases WHERE job_id = %s AND lease_token = %s::uuid RETURNING job_id",
                (job_id, lease_token),
            ).fetchone()
        return row is not None

    def recover_expired(self, job_id: str, now: datetime) -> bool:
        if not job_id.strip() or now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("job_id must be nonblank and now timezone-aware")
        with self._connection_factory() as connection:
            row = connection.execute(
                "DELETE FROM generation_worker_leases WHERE job_id = %s AND expires_at <= %s RETURNING job_id",
                (job_id, now),
            ).fetchone()
        return row is not None

    def current(self, job_id: str) -> WorkerLease | None:
        with self._connection_factory() as connection:
            row = connection.execute(
                """
                SELECT job_id, worker_id, lease_token, acquired_at, expires_at
                FROM generation_worker_leases WHERE job_id = %s AND expires_at > now()
                """,
                (job_id,),
            ).fetchone()
        return self._lease(row) if row else None

    @staticmethod
    def _lease(row) -> WorkerLease:
        return WorkerLease(row[0], row[1], str(row[2]), row[3], row[4])

    @staticmethod
    def _validate(job_id, worker_id, now, lease_duration):
        if not job_id.strip() or not worker_id.strip():
            raise ValueError("job_id and worker_id cannot be blank")
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("now must be timezone-aware")
        if lease_duration <= timedelta(0):
            raise ValueError("lease_duration must be positive")
