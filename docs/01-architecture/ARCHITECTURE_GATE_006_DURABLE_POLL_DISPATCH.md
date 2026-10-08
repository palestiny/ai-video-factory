# Architecture Gate 006 — Durable Poll Dispatch and Scheduling Contract

**Status: DESIGN CONTRACT RETAINED — implementation must follow approved Gate 007.**

Gate 007 selected PostgreSQL plus a PostgreSQL-backed delayed work queue on 2026-10-09. That decision resolves Gate 006's open technology choice for the MVP; it does **not** mean this gate has passed. The existing worker still schedules through the application queue port after the provider-operation transaction, so the atomic state-plus-intent invariant is not yet implemented.

Decision record: [Architecture Gate 007 — Persistence and Durable Scheduling Decision](ARCHITECTURE_GATE_007_PERSISTENCE_AND_SCHEDULING_DECISION.md).

## Goal

Specify the correctness contract for scheduling future provider-status checks so that a durable non-terminal operation cannot be stranded between a database commit and queue publication.

## Failure window being closed

The current async worker persists provider-operation state and then calls `enqueue_after(job_id, delay)`. A crash between these operations can strand a non-terminal operation. A crash after scheduling but before acknowledging the source delivery can also create duplicate messages. The production design must durably record the next work intent in the same database transaction as the state transition that requires it, then tolerate at-least-once delivery.

## Required invariants

1. **Atomic intent creation.** When a status observation requires another poll, durable operation state and the next poll intent are committed in one local PostgreSQL transaction.
2. **Stable intent identity.** A logical poll generation has one stable key derived from `(provider_name, provider_operation_id, poll_generation)`. Repeated handling must not create unbounded new intents for the same generation.
3. **At-least-once delivery.** Duplicate delivery is expected and consumers remain idempotent.
4. **No premature acknowledgement.** The worker acknowledges its source delivery only after the operation observation and any required next-poll intent are durably committed.
5. **Terminal suppression.** A terminal provider operation must not create new poll generations. Previously queued messages for terminal/cancelled jobs safely no-op or reconcile durable state.
6. **Stale generation protection.** A delayed message from poll generation N must not supersede a newer accepted observation or create generation N+1 more than once.
7. **Concurrency protection.** Concurrent workers cannot commit duplicate logical poll generations. Enforce this with a unique constraint and transactional conflict handling.
8. **Bounded retry policy.** Dispatch failures retry with bounded backoff and observable attempt counts; persistent failures become alertable and recoverable.
9. **No provider call in queue claiming.** Queue claiming/dispatch only returns due work. Provider polling and lifecycle transitions remain application-worker responsibilities.
10. **Cancellation safety.** Cancellation does not require deleting already-published messages. Workers re-read durable state and must not resurrect a cancelled job.
11. **Lease fencing.** Claims have expiring leases and fresh tokens; stale owners cannot ACK or mutate the current claim.
12. **Timezone semantics.** Due times are timezone-aware UTC instants at the application boundary and stored as PostgreSQL `TIMESTAMPTZ`.

## Approved MVP implementation shape

- PostgreSQL is the source of truth for job state and scheduled work.
- Insert a work item in the same transaction as the state change that requires it.
- Claim due items atomically using `FOR UPDATE SKIP LOCKED` or an equivalently tested strategy.
- Use the existing `generation_work_items` schema as an initial baseline, subject to migration and adapter tests.
- Keep the queue behind an application port; do not introduce a separate broker for the MVP.
- A work item's stable `intent_key` represents one logical intent, not one delivery attempt. Redelivery increments delivery metadata without changing that identity.

The initial schema is not proof of the above semantics. The adapter must define and test the exact ownership/ACK protocol and must not acknowledge stale claim tokens.

## Required flow

1. Worker reads current durable job and provider operation.
2. Provider status is normalized.
3. In one database transaction, persist the observation and, if non-terminal and polling is permitted, insert or reuse the unique next-poll intent with its due time.
4. Commit the transaction.
5. Acknowledge the source delivery only after the commit succeeds.
6. Claim due work atomically with a lease token and expiry; competing claimers cannot own the same active claim.
7. ACK/release validates the current token. Expired claims become recoverable.
8. If the process crashes after commit but before ACK, redelivery is safe because the logical intent key is stable and unique.
9. A consumer treats the message as a hint, re-reads durable state, and checks terminal/cancellation/generation state before polling.

## Required tests against real PostgreSQL

1. Operation-state commit and intent creation are atomic.
2. Rollback leaves neither a partially advanced operation nor a poll intent.
3. Duplicate insertion for the same poll generation resolves to one logical intent.
4. Concurrent workers cannot create two intents for the same generation.
5. Two claimers cannot own the same active due item.
6. Publish/ACK or worker crash windows recover through redelivery without losing the intent.
7. A failed ACK/release with a stale token is rejected.
8. An expired claim can be reclaimed with a new token; the old token cannot ACK it.
9. A terminal operation cannot create another poll intent.
10. A stale poll-generation message cannot regress operation state.
11. A cancelled job remains cancelled when stale work arrives.
12. Retry exhaustion remains observable and preserves recoverable intent metadata.
13. Restart recovery handles pending work and expired claims.
14. Queue age, delivery attempts, and retry/dead-letter state are observable.

## Remaining gaps

- The current `GenerationJobQueue` port exposes `enqueue_after(job_id, delay)` and does not carry a stable intent key or poll generation.
- The current async worker performs scheduling separately from persistence commit.
- The PostgreSQL migration exists, but no production PostgreSQL repository/transaction adapter or queue implementation is established yet.
- The migration smoke test verifies schema application only; it does not prove atomicity, concurrency, fencing, or crash recovery.
- Webhook-triggered immediate checks must eventually use the same durable intent contract rather than a separate scheduling path.

## Acceptance criteria

Gate 006 remains **NOT PASSED** until the application port carries stable logical intent identity, operation state and work intent are committed atomically, and the concurrency/crash-window tests above pass against the real PostgreSQL adapter. Production readiness remains unproven until operational recovery and metrics are also verified.
