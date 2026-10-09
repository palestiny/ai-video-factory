# Architecture Gate 006 — Durable Poll Dispatch and Scheduling Contract

**Status: DESIGN CONTRACT RETAINED — implementation must follow approved Gate 007.**

Gate 007 selected PostgreSQL plus a PostgreSQL-backed delayed work queue on 2026-10-09. The async worker now asks the provider-operation lifecycle to persist the next poll intent in the same transaction as the provider status update, and ACKs only after that transaction succeeds. This is implemented and covered by in-memory transaction tests, but the complete PostgreSQL persistence composition and real-PostgreSQL operation-state/intent atomicity test are still missing; Gate 006 has **not** passed.

Decision record: [Architecture Gate 007 — Persistence and Durable Scheduling Decision](ARCHITECTURE_GATE_007_PERSISTENCE_AND_SCHEDULING_DECISION.md).

## Goal

Specify the correctness contract for scheduling future provider-status checks so that a durable non-terminal operation cannot be stranded between a database commit and queue publication.

## Failure window being closed

The original crash window was operation-state commit followed by `enqueue_after(job_id, delay)`. The async worker has since been changed to ask the lifecycle to persist the next poll intent in the same execution transaction, and to ACK the source message only after commit. The remaining proof obligation is a production PostgreSQL transaction adapter and real-PostgreSQL atomicity/concurrency testing.

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

## Verified implementation findings (2026-10-09)

- The async worker calls `ProviderOperationLifecycle.poll(operation)`, and that lifecycle method saves the observed operation and commits its transaction internally.
- The prior worker call graph had a commit-then-enqueue crash window. The current implementation removes the separate enqueue step for provider polls by writing the poll intent inside the lifecycle transaction.
- `ProviderOperation` defines a persisted `poll_generation` field (default `0`, non-negative, with a PostgreSQL column/check constraint). The lifecycle now advances it only for non-terminal observations when scheduling is requested, and inserts the matching generation-specific `WorkIntent` before committing. Queue messages carry intent kind/key and provider-operation generation; stale-generation deliveries return the authoritative operation without another provider poll.
- The execution transaction port now includes a `WorkIntentRepository` contract. `WorkIntent` defines a stable provider-poll key based on provider, operation ID, and generation; the in-memory repository is idempotent for identical intents and rejects identity reuse with different contents.
- The in-memory transaction snapshot now includes work intents, and focused unit tests cover stable identity, idempotent insertion, identity conflict, timezone-aware due times, and rollback. This is deterministic contract testing, **not evidence of PostgreSQL atomicity**.
- The migration now stores `intent_kind`, `provider`, `operation_id`, and `generation` alongside the queue row, validates complete positive provider-poll identity, and has a partial unique index on `(provider, operation_id, generation)` for provider-poll intents. This is schema-level groundwork now mapped by `PostgresWorkIntentRepository`; complete PostgreSQL operation-state transaction composition and concurrency proof remain outstanding. The migration also enforces a unique nonblank `intent_key` and lease metadata consistency.
- GitHub Actions run #336 passed after the `poll_generation` schema test was corrected. The work-intent unit-test runs for commit `cf06b553` also passed. Passing schema and queue adapter tests still do not prove operation-state/work-intent atomicity, concurrent status monotonicity, or end-to-end crash recovery.

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

- `ProviderOperation` now carries a non-negative optimistic `version`; lifecycle polling advances it with each accepted observation. The PostgreSQL provider-operation adapter uses compare-and-swap updates against the previous version and rejects stale concurrent writers. Terminal rows cannot be changed to a different status by a stale write.
- `PostgresProviderOperationRepository` and `PostgresWorkIntentRepository` are caller-connection adapters that do not commit independently. New PostgreSQL integration tests exercise operation/result round-trip, operation-state + poll-intent commit/rollback on one connection, and a two-connection stale-write race. The first CI run exposed one existing unit test that did not advance the new version token when saving; that test has been corrected. Backend Tests run [#429](https://github.com/palestiny/ai-video-factory/actions/runs/37863399866) passed on commit `bf13b9b678a12fa24373da6a957a854ffd00752e`, including the new PostgreSQL operation-state/work-intent atomicity and stale-write tests.

## PostgreSQL transaction composition update (2026-10-09)

- Added `PostgresPersistenceTransaction`, which composes the job, append-only attempt-history, event, idempotency, provider-operation, and work-intent adapters over the same psycopg connection. It is the explicit commit/rollback owner; individual repositories do not open separate connections or commit independently.
- Added PostgreSQL integration coverage for operation-state + work-intent + attempt + event commit/rollback, active lease validation, and submission idempotency reservation followed by job insertion. The idempotency foreign key is deferred until commit because the existing submission use case reserves the key before inserting the job.
- Backend Tests run [#437](https://github.com/palestiny/ai-video-factory/actions/runs/37864087010) passed on commit `61a08f0d10f8822518291862ed56ef9d65552067`. A follow-up change avoids holding the queue-row lock across external provider calls and revalidates/locks lease ownership at commit time; run [#439](https://github.com/palestiny/ai-video-factory/actions/runs/37864159247) passed. A regression test then forced lease expiry after the initial check and verified commit rejection; run [#443](https://github.com/palestiny/ai-video-factory/actions/runs/37864215918) also passed.
- This is evidence for repository composition and transaction atomicity, not yet proof of complete worker crash/restart recovery. Gate 006 remains **NOT PASSED** until the worker lifecycle is exercised through process interruption/restart and remaining terminal/stale-delivery invariants are covered end-to-end.
- Backend Tests run [#453](https://github.com/palestiny/ai-video-factory/actions/runs/37864994909) passed on commit `1014ed6ef1a0599e0ac2236535875a9ce6d1c12d`. The composed-transaction integration suite verifies shared commit/rollback for operation state, poll intent, attempt history, and events; verifies submission idempotency reservation and job insertion; and checks lease ownership revalidation at commit. The job repository now locks the aggregate row with `SELECT ... FOR UPDATE` before read/modify/save. This still does not replace a full worker process crash/restart test.
- The transaction/worker lease boundary has now been aligned: `PostgresWorkerLeaseRepository` persists one active lease per job with fresh-token reclamation, renewal/release fencing, and expiry recovery; the persistence transaction validates the worker lease token at commit time, and the async worker registers the same token with its transaction. Backend Tests run [#475](https://github.com/palestiny/ai-video-factory/actions/runs/37865408121) passed on commit `02c98024a7df74aa99ba9da0686de736090ebc36`, including tests for exclusive claims, expiry/reclaim with a fresh token, stale-token rejection, and transaction commit rejection after lease expiry.

- A new recovery integration test simulates the process dying after the operation-state + poll-intent commit but before source ACK. A fresh queue instance reclaims the expired source lease, observes the committed intent/state, rejects the stale worker token, and ACKs using the new token. Backend Tests run [#447](https://github.com/palestiny/ai-video-factory/actions/runs/37864332220) passed with this test included. This validates the commit-before-ACK recovery seam at the PostgreSQL queue/transaction boundary; it is not yet a full provider-worker restart test.

- The new composed-transaction regression tests also exercise job/event/provider-operation/work-intent commit and rollback, plus lease expiry after the initial ownership check. The first CI attempt exposed invalid expired-lease fixtures that violated the schema invariant `expires_at > acquired_at`; the fixtures were corrected to move both timestamps while preserving their ordering. Backend Tests run [#497](https://github.com/palestiny/ai-video-factory/actions/runs/37870058574) passed on commit `6f9b9bd1be73109c0440531c5c3db514c5971db4` with **163 passed**. This is additional transaction/lease evidence; Gate 006 remains **NOT PASSED** pending complete worker-process restart wiring and end-to-end invariant coverage.

- A PostgreSQL-backed end-to-end worker recovery test now injects an abrupt interruption immediately after the operation-state + poll-intent commit and before source ACK, then creates fresh queue/worker instances. It verifies source redelivery is fenced, the stale initial delivery does not poll again, the durable poll intent is processed, and the job/attempt reach `SUCCEEDED` without duplicate provider submission. Backend Tests run [#499](https://github.com/palestiny/ai-video-factory/actions/runs/37870360710) passed on commit `883a2265e48027465bee106980817d48cb5b7b85` with **164 passed**. This closes the previously missing worker-level crash/ACK recovery seam in the integration suite; Gate 006 remains **NOT PASSED** until production connection-lifecycle ownership is explicit and remaining terminal/stale-delivery concurrency cases are verified.

## Remaining gaps

- The repository adapters and transaction composition exist, but the production composition root/factory that manages connection lifetime for an entire worker delivery is not yet complete. `PostgresPersistenceTransaction` intentionally receives a caller-owned connection; production worker wiring must guarantee cleanup without closing a connection between multiple commits in one async delivery.
- The PostgreSQL worker lease repository now provides per-job exclusive leases and token fencing, while the durable queue has its own delivery claim token. These are intentionally separate scopes; end-to-end wiring must verify both tokens are validated at the right boundaries and neither lease can be mistaken for the other.
- Existing PostgreSQL tests verify operation/intent/attempt/event commit and rollback, job submission idempotency, lease expiry/reclaim, and stale-token rejection. A full process-interruption/restart test through `ExecuteAsyncProviderDelivery` using PostgreSQL-backed queue, worker leases, and transaction composition is still required.
- Verify all terminal/stale-generation/cancellation paths under concurrent PostgreSQL workers, including crash after durable poll scheduling but before source ACK, and ensure a restarted worker recovers without duplicate provider submission or a lost poll.
- The queue port still exposes `enqueue_after(job_id, delay)` for generic execution work; provider polls correctly use `WorkIntentRepository` inside the operation-state transaction. Webhook-triggered immediate checks must eventually use the same durable intent contract rather than a separate scheduling path.
- Operational readiness still requires observability for queue age, delivery attempts, expired/reclaimed leases, poll retries, dead-letter state, and recovery outcomes.

## Acceptance criteria

Gate 006 remains **NOT PASSED** until production connection lifecycle/composition is implemented, the full async worker is exercised through a real PostgreSQL crash/restart scenario, and remaining terminal/stale-delivery invariants are verified under concurrency. Repository-level atomicity and lease tests have passed, but they are not by themselves proof of end-to-end worker recovery.
