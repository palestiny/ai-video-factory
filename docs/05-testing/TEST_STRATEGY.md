# Test Strategy

## Test pyramid

### Unit tests
Pure domain state transitions, validation, idempotency normalization, retry policy, and cost calculations.

### Application tests
Use-case behavior with fake ports. No vendor APIs.

### Contract tests
Provider adapters must satisfy the normalized capability contracts.

### Integration tests
Persistence, queue, object storage, and rendering integration.

### End-to-end tests
One deterministic fixture exercises the full pipeline with fake providers.

## External API policy

Tests must not require paid AI APIs.

Fake providers are first-class test infrastructure.

## Required reliability tests

- duplicate idempotency submission
- concurrent idempotency race
- retryable failure
- non-retryable failure
- retry limit reached
- timeout
- worker recovery
- successful replay
- selective scene regeneration
- cost recorded once per attempt
