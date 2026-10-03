from __future__ import annotations

from dataclasses import dataclass

import pytest

from app.application.execution import ExecuteGenerationCommand, ExecuteGenerationJob, ProviderExecutionError
from app.application.ports import GenerationRequest, GenerationResult
from app.domain.events import JobEvent
from app.domain.failure import Failure, FailureCode
from app.domain.generation import GenerationAttempt, GenerationJob


class JobRepo:
    def __init__(self, job: GenerationJob) -> None:
        self.job = job

    def get(self, job_id: str) -> GenerationJob | None:
        return self.job if self.job.job_id == job_id else None


class AttemptRepo:
    def __init__(self) -> None:
        self.items: dict[str, list[GenerationAttempt]] = {}

    def add(self, attempt: GenerationAttempt) -> None:
        self.items.setdefault(attempt.attempt_id, []).append(attempt)

    def complete(self, attempt: GenerationAttempt) -> None:
        self.items.setdefault(attempt.attempt_id, []).append(attempt)


@dataclass
class Transaction:
    jobs: JobRepo
    attempts: AttemptRepo
    events: list[JobEvent]
    commit_error: Exception | None = None
    rollback_calls: int = 0

    def append_event(self, event: JobEvent) -> None:
        self.events.append(event)

    def commit(self) -> None:
        if self.commit_error is not None:
            raise self.commit_error

    def rollback(self) -> None:
        self.rollback_calls += 1


class FakeProvider:
    provider_name = "fake-video"

    def __init__(self, result: GenerationResult | None = None, error: Exception | None = None) -> None:
        self.result = result
        self.error = error
        self.requests: list[GenerationRequest] = []

    def generate(self, request: GenerationRequest) -> GenerationResult:
        self.requests.append(request)
        if self.error is not None:
            raise self.error
        assert self.result is not None
        return self.result


class Resolver:
    def __init__(self, provider: FakeProvider) -> None:
        self.provider = provider

    def resolve(self, capability: str) -> FakeProvider:
        assert capability == "video"
        return self.provider


def make_service(provider: FakeProvider) -> tuple[ExecuteGenerationJob, Transaction]:
    job = GenerationJob.create("job-1", "video", "scene-1/v1")
    tx = Transaction(JobRepo(job), AttemptRepo(), [])
    return ExecuteGenerationJob(tx, Resolver(provider)), tx


def test_success_completes_job_attempt_and_events() -> None:
    provider = FakeProvider(GenerationResult("fake-video", "op-1", ("asset-1",)))
    service, tx = make_service(provider)

    result = service.execute(ExecuteGenerationCommand("job-1", {"prompt": "cinematic shot"}))

    assert result.failure is None
    assert result.generation is not None
    assert result.job.status.value == "SUCCEEDED"
    assert result.attempt.status.value == "SUCCEEDED"
    assert result.attempt.attempt_number == 1
    assert [e.event_type.value for e in tx.events] == ["STARTED", "SUCCEEDED"]
    assert provider.requests[0].idempotency_key == "scene-1/v1"


def test_normalized_provider_failure_is_recorded_without_retrying_here() -> None:
    failure = Failure.from_code(
        "RATE_LIMITED", "provider throttled the request",
        provider="fake-video", provider_operation_id="op-2",
    )
    provider = FakeProvider(error=ProviderExecutionError(failure))
    service, tx = make_service(provider)

    result = service.execute(ExecuteGenerationCommand("job-1", {"prompt": "retry me"}))

    assert result.failure is failure
    assert result.generation is None
    assert result.job.status.value == "FAILED"
    assert result.attempt.status.value == "FAILED"
    assert result.attempt.failure_code == FailureCode.RATE_LIMITED.value
    assert result.attempt.provider_operation_id == "op-2"
    assert tx.events[-1].failure_code == FailureCode.RATE_LIMITED.value


def test_unknown_provider_exception_is_normalized_to_unknown_failure() -> None:
    service, _ = make_service(FakeProvider(error=RuntimeError("boom")))

    result = service.execute(ExecuteGenerationCommand("job-1", {}))

    assert result.failure is not None
    assert result.failure.code is FailureCode.UNKNOWN
    assert result.job.status.value == "FAILED"


def test_missing_job_is_rejected_before_provider_call() -> None:
    provider = FakeProvider(GenerationResult("fake-video", None, ()))
    job = GenerationJob.create("other-job", "video", "scene-1/v1")
    tx = Transaction(JobRepo(job), AttemptRepo(), [])
    service = ExecuteGenerationJob(tx, Resolver(provider))

    with pytest.raises(KeyError):
        service.execute(ExecuteGenerationCommand("job-1", {}))

    assert provider.requests == []


def test_commit_failure_rolls_back_execution_transaction() -> None:
    provider = FakeProvider(GenerationResult("fake-video", "op-3", ("asset-3",)))
    job = GenerationJob.create("job-1", "video", "scene-1/v1")
    tx = Transaction(JobRepo(job), AttemptRepo(), [], commit_error=RuntimeError("commit failed"))
    service = ExecuteGenerationJob(tx, Resolver(provider))

    with pytest.raises(RuntimeError, match="commit failed"):
        service.execute(ExecuteGenerationCommand("job-1", {"prompt": "commit failure"}))

    assert tx.rollback_calls == 1
