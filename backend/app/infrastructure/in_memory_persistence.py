from __future__ import annotations

from copy import deepcopy

from app.application.idempotency import InMemoryIdempotencyRepository
from app.application.persistence import (
    ExecutionPersistenceTransaction,
    GenerationAttemptRepository,
    GenerationJobRepository,
    SubmissionPersistenceTransaction,
)
from app.domain.events import JobEvent
from app.domain.generation import GenerationAttempt, GenerationJob


class InMemoryGenerationJobRepository(GenerationJobRepository):
    def __init__(self, items: dict[str, GenerationJob]) -> None:
        self._items = items

    def add(self, job: GenerationJob) -> None:
        if job.job_id in self._items:
            raise ValueError(f"generation job already exists: {job.job_id}")
        self._items[job.job_id] = deepcopy(job)

    def get(self, job_id: str) -> GenerationJob | None:
        job = self._items.get(job_id)
        return deepcopy(job) if job is not None else None


class InMemoryGenerationAttemptRepository(GenerationAttemptRepository):
    def __init__(self, items: dict[str, list[GenerationAttempt]]) -> None:
        self._items = items

    def add(self, attempt: GenerationAttempt) -> None:
        self._items.setdefault(attempt.attempt_id, []).append(deepcopy(attempt))

    def complete(self, attempt: GenerationAttempt) -> None:
        history = self._items.get(attempt.attempt_id)
        if not history:
            raise KeyError(f"attempt not found: {attempt.attempt_id}")
        history.append(deepcopy(attempt))

    def history(self, attempt_id: str) -> tuple[GenerationAttempt, ...]:
        return tuple(deepcopy(self._items.get(attempt_id, ())))


class InMemoryPersistenceTransaction(
    SubmissionPersistenceTransaction,
    ExecutionPersistenceTransaction,
):
    """Deterministic transaction test double.

    It snapshots durable state at construction. Commit publishes staged state;
    rollback restores the snapshot. It deliberately models the application
    transaction contract without selecting a database or ORM.
    """

    def __init__(
        self,
        jobs: dict[str, GenerationJob] | None = None,
        attempts: dict[str, list[GenerationAttempt]] | None = None,
        events: list[JobEvent] | None = None,
        idempotency: InMemoryIdempotencyRepository | None = None,
    ) -> None:
        self._jobs = jobs if jobs is not None else {}
        self._attempts = attempts if attempts is not None else {}
        self._events = events if events is not None else []
        self._idempotency = idempotency or InMemoryIdempotencyRepository()
        self.jobs = InMemoryGenerationJobRepository(self._jobs)
        self.attempts = InMemoryGenerationAttemptRepository(self._attempts)
        self.idempotency = self._idempotency
        self._snapshot = self._capture()
        self.commit_count = 0
        self.rollback_count = 0
        self.fail_commit = False

    @property
    def events(self) -> tuple[JobEvent, ...]:
        return tuple(self._events)

    def jobs_state(self) -> dict[str, GenerationJob]:
        return deepcopy(self._jobs)

    def attempts_state(self) -> dict[str, list[GenerationAttempt]]:
        return deepcopy(self._attempts)

    def append_event(self, event: JobEvent) -> None:
        self._events.append(deepcopy(event))

    def commit(self) -> None:
        if self.fail_commit:
            raise RuntimeError("commit failed")
        self._snapshot = self._capture()
        self.commit_count += 1

    def rollback(self) -> None:
        self._restore(self._snapshot)
        self.rollback_count += 1

    def _capture(self) -> tuple[dict, dict, list, dict]:
        return (
            deepcopy(self._jobs),
            deepcopy(self._attempts),
            deepcopy(self._events),
            self._idempotency.snapshot(),
        )

    def _restore(self, snapshot: tuple[dict, dict, list, dict]) -> None:
        jobs, attempts, events, reservations = deepcopy(snapshot)
        self._jobs.clear()
        self._jobs.update(jobs)
        self._attempts.clear()
        self._attempts.update(attempts)
        self._events.clear()
        self._events.extend(events)
        self._idempotency.restore(reservations)
