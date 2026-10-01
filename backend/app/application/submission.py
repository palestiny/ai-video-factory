from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Protocol

from app.application.idempotency import IdempotencyRepository, ReservationStatus
from app.domain.events import JobEvent, JobEventType
from app.domain.generation import GenerationJob


class GenerationJobRepository(Protocol):
    def add(self, job: GenerationJob) -> None:
        ...

    def get(self, job_id: str) -> GenerationJob | None:
        ...


class Transaction(Protocol):
    idempotency: IdempotencyRepository
    jobs: GenerationJobRepository

    def append_event(self, event: JobEvent) -> None:
        ...

    def commit(self) -> None:
        ...

    def rollback(self) -> None:
        ...


@dataclass(frozen=True)
class SubmitGenerationCommand:
    job_id: str
    capability: str
    idempotency_key: str
    request_fingerprint: str


@dataclass(frozen=True)
class SubmitGenerationResult:
    job: GenerationJob
    created: bool


class SubmitGenerationJob:
    """Create or resolve one durable logical generation job.

    The transaction boundary is deliberately explicit: the idempotency
    reservation, job creation, and CREATED event must commit together.
    """

    def __init__(self, transaction: Transaction) -> None:
        self._transaction = transaction

    def execute(self, command: SubmitGenerationCommand) -> SubmitGenerationResult:
        reservation = self._transaction.idempotency.reserve(
            key=command.idempotency_key,
            request_fingerprint=command.request_fingerprint,
            job_id=command.job_id,
        )

        if reservation.status is ReservationStatus.EXISTING:
            existing = self._transaction.jobs.get(reservation.job_id)
            if existing is None:
                self._transaction.rollback()
                raise RuntimeError(
                    "idempotency reservation points to a missing generation job"
                )
            self._transaction.rollback()
            return SubmitGenerationResult(job=existing, created=False)

        job = GenerationJob.create(
            job_id=reservation.job_id,
            capability=command.capability,
            idempotency_key=reservation.key,
        )
        self._transaction.jobs.add(job)
        self._transaction.append_event(
            JobEvent(
                event_id=f"{job.job_id}:created",
                job_id=job.job_id,
                event_type=JobEventType.CREATED,
                occurred_at=datetime.now(timezone.utc),
            )
        )
        self._transaction.commit()
        return SubmitGenerationResult(job=job, created=True)
