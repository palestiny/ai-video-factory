from __future__ import annotations

from typing import Protocol

from app.application.idempotency import IdempotencyRepository
from app.domain.events import JobEvent
from app.domain.generation import GenerationAttempt, GenerationJob


class GenerationJobRepository(Protocol):
    """Durable repository for logical generation jobs."""

    def add(self, job: GenerationJob) -> None:
        ...

    def get(self, job_id: str) -> GenerationJob | None:
        ...

    def save(self, job: GenerationJob) -> None:
        """Stage the current aggregate state for the active transaction."""
        ...


class GenerationAttemptRepository(Protocol):
    """Durable repository for immutable generation attempt history.

    A terminal attempt is recorded as a new persisted version of the same
    logical attempt identity. Implementations must preserve the history and
    must not mutate an already-committed attempt in place.
    """

    def add(self, attempt: GenerationAttempt) -> None:
        ...

    def complete(self, attempt: GenerationAttempt) -> None:
        """Append the terminal record for an existing immutable attempt."""
        ...


class JobEventStore(Protocol):
    """Append-only persistence boundary for job lifecycle events."""

    def append(self, event: JobEvent) -> None:
        ...


from app.application.provider_operation_repository import ProviderOperationRepository

class PersistenceTransaction(Protocol):
    """Atomic persistence boundary shared by application use cases.

    Production implementations must make all writes performed through one
    transaction durable together, or roll them all back.
    """

    jobs: GenerationJobRepository

    def append_event(self, event: JobEvent) -> None:
        ...

    def commit(self) -> None:
        ...

    def rollback(self) -> None:
        ...


class SubmissionPersistenceTransaction(PersistenceTransaction, Protocol):
    idempotency: IdempotencyRepository


class ExecutionPersistenceTransaction(PersistenceTransaction, Protocol):
    attempts: GenerationAttemptRepository
    provider_operations: ProviderOperationRepository

    def assert_lease_owner(self, job_id: str, lease_token: str) -> None:
        """Reject terminal persistence when the worker no longer owns the lease."""
        ...
