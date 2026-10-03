from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Protocol

from app.application.persistence import ExecutionPersistenceTransaction
from app.application.ports import GenerationRequest, GenerationResult
from app.domain.events import JobEvent, JobEventType
from app.domain.failure import Failure
from app.domain.generation import GenerationAttempt, GenerationJob


class GenerationProvider(Protocol):
    provider_name: str

    def generate(self, request: GenerationRequest) -> GenerationResult:
        ...


class GenerationProviderResolver(Protocol):
    def resolve(self, capability: str) -> GenerationProvider:
        ...


@dataclass(frozen=True)
class ExecuteGenerationCommand:
    job_id: str
    inputs: dict[str, object]
    references: tuple[str, ...] = ()
    constraints: dict[str, object] | None = None


@dataclass(frozen=True)
class ExecuteGenerationResult:
    job: GenerationJob
    attempt: GenerationAttempt
    generation: GenerationResult | None
    failure: Failure | None


class ProviderExecutionError(RuntimeError):
    def __init__(self, failure: Failure) -> None:
        super().__init__(failure.message)
        self.failure = failure


class ExecuteGenerationJob:
    """Execute one provider attempt; retry scheduling stays outside this use case."""

    def __init__(self, transaction: ExecutionPersistenceTransaction, providers: GenerationProviderResolver) -> None:
        self._transaction = transaction
        self._providers = providers

    def execute(self, command: ExecuteGenerationCommand) -> ExecuteGenerationResult:
        job = self._transaction.jobs.get(command.job_id)
        if job is None:
            raise KeyError(f"generation job not found: {command.job_id}")

        provider = self._providers.resolve(job.capability)
        job.start()
        started_at = datetime.now(timezone.utc)
        attempt = GenerationAttempt.started(
            attempt_id=f"{job.job_id}:attempt-{job.attempt_count}",
            job_id=job.job_id,
            attempt_number=job.attempt_count,
            provider=provider.provider_name,
            started_at=started_at,
        )
        self._transaction.attempts.add(attempt)
        self._transaction.append_event(JobEvent(
            event_id=f"{job.job_id}:started:{job.attempt_count}",
            job_id=job.job_id,
            event_type=JobEventType.STARTED,
            occurred_at=started_at,
            attempt_number=job.attempt_count,
        ))

        request = GenerationRequest(
            job_id=job.job_id,
            capability=job.capability,
            inputs=command.inputs,
            references=command.references,
            constraints=command.constraints or {},
            idempotency_key=job.idempotency_key,
        )

        try:
            generation = provider.generate(request)
        except ProviderExecutionError as exc:
            return self._fail(job, attempt, provider.provider_name, exc.failure)
        except Exception as exc:
            return self._fail(
                job,
                attempt,
                provider.provider_name,
                Failure.from_code("UNKNOWN", str(exc) or "provider execution failed", provider=provider.provider_name),
            )

        completed_at = datetime.now(timezone.utc)
        job.succeed()
        completed_attempt = GenerationAttempt.succeeded(
            attempt_id=attempt.attempt_id,
            job_id=job.job_id,
            attempt_number=attempt.attempt_number,
            provider=generation.provider,
            started_at=attempt.started_at,
            completed_at=completed_at,
            provider_operation_id=generation.provider_operation_id,
        )
        self._transaction.attempts.complete(completed_attempt)
        self._transaction.append_event(JobEvent(
            event_id=f"{job.job_id}:succeeded:{job.attempt_count}",
            job_id=job.job_id,
            event_type=JobEventType.SUCCEEDED,
            occurred_at=completed_at,
            attempt_number=job.attempt_count,
        ))
        self._commit_or_rollback()
        return ExecuteGenerationResult(job, completed_attempt, generation, None)

    def _fail(
        self,
        job: GenerationJob,
        attempt: GenerationAttempt,
        provider: str,
        failure: Failure,
    ) -> ExecuteGenerationResult:
        completed_at = datetime.now(timezone.utc)
        job.fail(failure.code.value)
        failed_attempt = GenerationAttempt.failed(
            attempt_id=attempt.attempt_id,
            job_id=job.job_id,
            attempt_number=attempt.attempt_number,
            provider=provider,
            started_at=attempt.started_at,
            completed_at=completed_at,
            failure_code=failure.code.value,
            provider_operation_id=failure.provider_operation_id,
        )
        self._transaction.attempts.complete(failed_attempt)
        self._transaction.append_event(JobEvent(
            event_id=f"{job.job_id}:failed:{job.attempt_count}",
            job_id=job.job_id,
            event_type=JobEventType.FAILED,
            occurred_at=completed_at,
            attempt_number=job.attempt_count,
            failure_code=failure.code.value,
        ))
        self._commit_or_rollback()
        return ExecuteGenerationResult(job, failed_attempt, None, failure)

    def _commit_or_rollback(self) -> None:
        try:
            self._transaction.commit()
        except Exception:
            self._transaction.rollback()
            raise
