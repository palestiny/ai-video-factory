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

## Idempotency Repository Contract

The logical generation job is identified by a normalized idempotency key. Reservation is an atomic repository operation, not a read-then-write sequence.

### Contract

reserve(key, request_fingerprint, job_id) must:

1. Normalize the key before lookup.
2. Create exactly one reservation when the key is unused.
3. Return the existing logical job when the normalized key and request fingerprint match.
4. Raise IdempotencyConflict when the key is reused for a different request fingerprint.
5. Be atomic for concurrent callers; at most one caller may create the reservation.

### Why the fingerprint exists

An idempotency key alone proves request identity only if the caller guarantees correct key construction. The repository therefore records a normalized request fingerprint so accidental key reuse cannot silently attach a new request to an old job.

### Boundary

The application owns the reservation contract. Persistence-specific uniqueness constraints, transactions, locking, or compare-and-set mechanisms remain infrastructure concerns.

The in-memory implementation is a deterministic test double only; it is not the production persistence strategy.
