# Domain Contract Implementation

## First implemented slice

The first TDD slice establishes the GenerationJob aggregate behavior:

- QUEUED → RUNNING
- RUNNING → SUCCEEDED
- RUNNING → FAILED
- FAILED → RETRYING
- RETRYING → RUNNING
- terminal states reject invalid transitions

## Retry semantics

Retryability is determined by normalized error category plus attempt count and policy.

Retryable categories currently include:
- RATE_LIMITED
- TIMEOUT
- PROVIDER_FAILURE
- TRANSIENT_NETWORK

CONTENT_REJECTED, authentication failures, and invalid requests are not retryable by default.

## Important boundary

The domain decides whether a retry is allowed. Scheduling the actual retry delay, queueing work, and executing providers remain application/infrastructure responsibilities.

## Next TDD slices

1. Immutable GenerationAttempt
2. Idempotency repository contract
3. Concurrent idempotency race semantics
4. Failure normalization
5. Job events
6. Provider ports and deterministic fakes


## GenerationAttempt

Each execution attempt is an immutable record owned by the logical generation job.

Required fields:
- attempt_id
- job_id
- attempt_number
- provider
- started_at
- terminal completed_at when finished

Terminal outcome is represented by `SUCCEEDED` or `FAILED`. A failed attempt carries a normalized `failure_code` and may carry a provider operation identifier.

The logical `GenerationJob` remains mutable across retries; each retry creates a new immutable `GenerationAttempt`. This preserves execution history and prevents a retry from overwriting evidence from a previous provider call.
