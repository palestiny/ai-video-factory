# Reliability Model

## Idempotency

A logical generation request has a stable idempotency key.

Equivalent submissions with the same key must resolve to the same logical job rather than create duplicate work.

Idempotency normalization must be deterministic and tested.

## Attempts

A job may have multiple attempts. Attempts are immutable records of execution.

A retry creates a new attempt under the same logical job.

## Retry policy

Retry policy considers:
- retryable error category
- attempt count
- provider retry hints
- configured backoff
- overall deadline

No unbounded retry loops.

The PostgreSQL queue's delivery retry policy is bounded independently from the domain/provider retry policy:
- Default maximum delivery attempts: 5.
- Release/requeue uses exponential delay starting at 1 second, capped at 60 seconds.
- A failed final delivery or an expired final claim moves the work item to `DEAD` with `MAX_DELIVERY_ATTEMPTS_EXCEEDED`.
- Operators can inspect `delivery_attempt`, `last_error_code`, `state`, `created_at`, and `due_at` in `generation_work_items`. A user-facing metrics/alerting surface remains future work.

## Recovery

A worker restart must not silently lose a running job.

The system must be able to distinguish:
- queued
- running
- completed
- failed
- retrying

Recovery behavior will be specified before queue implementation.

## Selective regeneration

Regenerating scene N must not regenerate scenes 1..N-1 or N+1..end unless dependencies explicitly require it.

## Observability

Every job transition and attempt outcome produces a JobEvent.

Operational history must allow reconstruction of what was requested, attempted, returned, failed, retried, and ultimately selected.
