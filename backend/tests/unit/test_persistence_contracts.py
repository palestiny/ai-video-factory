from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import datetime, timezone

from app.application.persistence import (
    ExecutionPersistenceTransaction,
    GenerationAttemptRepository,
    GenerationJobRepository,
    JobEventStore,
    PersistenceTransaction,
    SubmissionPersistenceTransaction,
)
from app.domain.events import JobEvent
from app.domain.generation import GenerationAttempt, GenerationJob


def test_repository_contracts_expose_required_operations() -> None:
    assert callable(GenerationJobRepository.add)
    assert callable(GenerationJobRepository.get)
    assert callable(GenerationAttemptRepository.add)
    assert callable(GenerationAttemptRepository.complete)
    assert callable(JobEventStore.append)
    assert callable(PersistenceTransaction.append_event)
    assert callable(PersistenceTransaction.commit)
    assert callable(PersistenceTransaction.rollback)
    assert SubmissionPersistenceTransaction is not None
    assert ExecutionPersistenceTransaction is not None


def test_attempt_completion_is_append_only() -> None:
    assert not hasattr(GenerationAttemptRepository, "replace")
    assert callable(GenerationAttemptRepository.complete)


def test_attempts_are_required_to_be_immutable_domain_records() -> None:
    attempt = GenerationAttempt.started(
        attempt_id="job-1:attempt-1",
        job_id="job-1",
        attempt_number=1,
        provider="fake",
        started_at=datetime.now(timezone.utc),
    )

    try:
        attempt.status = attempt.status
    except FrozenInstanceError:
        pass
    else:
        raise AssertionError("GenerationAttempt must remain immutable")


def test_persistence_contract_is_separate_from_provider_ports() -> None:
    assert not hasattr(GenerationJob, "generate")
    assert not hasattr(JobEvent, "generate")
