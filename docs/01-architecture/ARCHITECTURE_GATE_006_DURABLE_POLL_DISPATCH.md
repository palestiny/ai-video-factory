# Architecture Gate 006 — Durable Poll Dispatch and Scheduling Contract

**Status: DESIGN CONTRACT RETAINED — implementation must follow approved Gate 007.**

Gate 007 selected PostgreSQL plus a PostgreSQL-backed delayed work queue on 2026-10-09. The PostgreSQL persistence composition binds jobs, attempt history, events, idempotency reservations, provider operations, and work intents to one caller-owned connection. Real-PostgreSQL integration tests cover atomic commit/rollback, commit-time execution-lease fencing, bounded queue retries/dead-lettering, and provider-worker restart after commit-before-ACK. The latest verified GitHub Actions run [#597](https://github.com/palestiny/ai-video-factory/actions/runs/37909122795) passed **173 tests** on commit `22ff38922d3a370d16affc565dcccb81d2cbe966`. Gate 006 has **not** passed: a passing suite is not a complete operational proof; provider reconciliation, remaining lifecycle invariants, and operational recovery procedures still require explicit evidence.

Decision record: [Architecture Gate 007 — Persistence and Durable Scheduling Decision](ARCHITECTURE_GATE_007_PERSISTENCE_AND_SCHEDULING_DECISION.md).

## Goal

Specify the correctness contract for scheduling future provider-status checks so that a durable non-terminal operation cannot be stranded between a database commit and queue publication.

## Failure window being closed

The original crash window was operation-state commit followed by `enqueue_after(job_id, delay)`. Provider poll state and its next durable work intent now share the same PostgreSQL transaction. Queue delivery claims and per-job execution leases are separate fencing layers: the former guards ACK/release; the latter guards state persistence and is revalidated at transaction commit. Remaining proof obligations focus on full worker restart/reconciliation and lifecycle edge cases.

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
- The migration stores `intent_kind`, `provider`, `operation_id`, and `generation` alongside queue rows, validates complete positive provider-poll identity, and enforces a partial unique index on `(provider, operation_id, generation)`. `PostgresWorkIntentRepository` and `PostgresProviderOperationRepository` share the transaction connection through `PostgresPersistenceTransaction`. The migration also defines `generation_worker_leases`; commit-time lease revalidation fences state writes after lease expiry.
- GitHub Actions run #336 passed after the `poll_generation` schema test was corrected. The work-intent unit-test runs for commit `cf06b553` also passed. Subsequent real-PostgreSQL tests now cover operation-state/work-intent atomicity, optimistic version conflicts, lease fencing, and simulated worker-process restart with fresh queue/worker objects. This evidence narrows the remaining gap but does not by itself prove every provider-specific reconciliation path or production operational procedure.

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
- `PostgresProviderOperationRepository` and `PostgresWorkIntentRepository` are caller-connection adapters that do not commit independently. `PostgresPersistenceTransaction` composes these with job, attempt, event, and idempotency adapters over the same connection. PostgreSQL integration tests cover all-staged-writes rollback/commit, idempotency-before-job insertion with a deferred FK, active/expired lease rejection, and revalidation when a lease expires before commit. Backend Tests run [#551](https://github.com/palestiny/ai-video-factory/actions/runs/37905479770) passed after the execution-lease SQL correction.
- A fresh `ProviderOperationLifecycle` instance now has PostgreSQL integration coverage for stale-generation replay and terminal-result replay after reconnect/restart: it returns the durable authoritative operation and does not call the provider again. Backend Tests run [#555](https://github.com/palestiny/ai-video-factory/actions/runs/37905709840) passed with this regression test.

## PostgreSQL transaction composition update (2026-10-09)

- Added `PostgresPersistenceTransaction`, which composes the job, append-only attempt-history, event, idempotency, provider-operation, and work-intent adapters over the same psycopg connection. It is the explicit commit/rollback owner; individual repositories do not open separate connections or commit independently.
- Added PostgreSQL integration coverage for operation-state + work-intent + attempt + event commit/rollback, active lease validation, and submission idempotency reservation followed by job insertion. The idempotency foreign key is deferred until commit because the existing submission use case reserves the key before inserting the job.
- Backend Tests run [#437](https://github.com/palestiny/ai-video-factory/actions/runs/37864087010) passed on commit `61a08f0d10f8822518291862ed56ef9d65552067`. A follow-up change avoids holding the queue-row lock across external provider calls and revalidates/locks lease ownership at commit time; run [#439](https://github.com/palestiny/ai-video-factory/actions/runs/37864159247) passed. A regression test then forced lease expiry after the initial check and verified commit rejection; run [#443](https://github.com/palestiny/ai-video-factory/actions/runs/37864215918) also passed.
- At the time of this run, the evidence covered repository composition and transaction atomicity only. The later end-to-end worker crash/restart evidence is recorded below; remaining terminal/stale-delivery invariants still prevent Gate 006 from passing.
- Backend Tests run [#453](https://github.com/palestiny/ai-video-factory/actions/runs/37864994909) passed on commit `1014ed6ef1a0599e0ac2236535875a9ce6d1c12d`. The composed-transaction integration suite verifies shared commit/rollback for operation state, poll intent, attempt history, and events; verifies submission idempotency reservation and job insertion; and checks lease ownership revalidation at commit. The job repository now locks the aggregate row with `SELECT ... FOR UPDATE` before read/modify/save. This still does not replace a full worker process crash/restart test.
- The transaction/worker lease boundary has now been aligned: `PostgresWorkerLeaseRepository` persists one active lease per job with fresh-token reclamation, renewal/release fencing, and expiry recovery; the persistence transaction validates the worker lease token at commit time, and the async worker registers the same token with its transaction. Backend Tests run [#475](https://github.com/palestiny/ai-video-factory/actions/runs/37865408121) passed on commit `02c98024a7df74aa99ba9da0686de736090ebc36`, including tests for exclusive claims, expiry/reclaim with a fresh token, stale-token rejection, and transaction commit rejection after lease expiry.

- A new recovery integration test simulates the process dying after the operation-state + poll-intent commit but before source ACK. A fresh queue instance reclaims the expired source lease, observes the committed intent/state, rejects the stale worker token, and ACKs using the new token. Backend Tests run [#447](https://github.com/palestiny/ai-video-factory/actions/runs/37864332220) passed with this test included. This validates the commit-before-ACK recovery seam at the PostgreSQL queue/transaction boundary; it is not yet a full provider-worker restart test.

- The new composed-transaction regression tests also exercise job/event/provider-operation/work-intent commit and rollback, plus lease expiry after the initial ownership check. The first CI attempt exposed invalid expired-lease fixtures that violated the schema invariant `expires_at > acquired_at`; the fixtures were corrected to move both timestamps while preserving their ordering. Backend Tests run [#497](https://github.com/palestiny/ai-video-factory/actions/runs/37870058574) passed on commit `6f9b9bd1be73109c0440531c5c3db514c5971db4` with **163 passed**. This is additional transaction/lease evidence; Gate 006 remains **NOT PASSED** pending complete worker-process restart wiring and end-to-end invariant coverage.

- A PostgreSQL-backed end-to-end worker recovery test now injects an abrupt interruption immediately after the operation-state + poll-intent commit and before source ACK, then creates fresh queue/worker instances. It verifies source redelivery is fenced, the stale initial delivery does not poll again, the durable poll intent is processed, and the job/attempt reach `SUCCEEDED` without duplicate provider submission. Backend Tests run [#499](https://github.com/palestiny/ai-video-factory/actions/runs/37870360710) passed on commit `883a2265e48027465bee106980817d48cb5b7b85` with **164 passed**. This closes the previously missing worker-level crash/ACK recovery seam in the integration suite; Gate 006 remains **NOT PASSED** until the worker's connection-lifecycle ownership is explicit through the transaction factory and delivery cleanup and remaining terminal/stale-delivery concurrency cases are verified.

- Backend Tests run [#573](https://github.com/palestiny/ai-video-factory/actions/runs/37908133064) passed with **167 passed**. The worker now re-registers lease ownership before each internal provider submit/poll transaction because every successful commit clears transaction-local lease checks. A PostgreSQL regression test expires the worker lease during provider polling and verifies the operation state and next-poll intent roll back together. Another test races cancellation against provider success and verifies the job remains `CANCELLED` and its attempt is terminalized as cancelled. The duplicate worker-lease table declaration was removed from the migration, and the schema smoke test asserts that table exists.

- Backend Tests run [#579](https://github.com/palestiny/ai-video-factory/actions/runs/37908327287) passed with **171 passed** after adding `build_postgres_async_worker`. The helper requires an explicit `database_url` or `DATABASE_URL`, and wires the PostgreSQL queue, worker-lease repository, and managed persistence-transaction factory without selecting a hosting vendor. Unit tests cover missing configuration and invalid durations. This is reusable runtime composition, not yet a long-running process entrypoint or deployment configuration.

- Backend Tests run [#587](https://github.com/palestiny/ai-video-factory/actions/runs/37908648773) passed with **173 passed** after implementing bounded PostgreSQL queue delivery retries. Default policy is 5 delivery attempts with exponential requeue delay from 1 second up to 60 seconds. A failed final release or an expired final claim moves the item to `DEAD` with `MAX_DELIVERY_ATTEMPTS_EXCEEDED`; integration tests cover both exhaustion paths. This bounds queue delivery retries; domain/provider retry policy and operational alerting remain separate concerns.

## Remaining gaps

- Worker transaction connection ownership is handled by an injected factory and delivery-finally cleanup. `build_postgres_async_worker` now builds the PostgreSQL queue, lease repository, and transaction factory from `database_url`/`DATABASE_URL`. Still open: a long-running process entrypoint, service shutdown/health lifecycle, and deployment configuration. No cloud vendor or connection pool implementation is selected.
- The PostgreSQL worker lease repository now provides per-job exclusive leases and token fencing, while the durable queue has its own delivery claim token. These are intentionally separate scopes; end-to-end wiring must verify both tokens are validated at the right boundaries and neither lease can be mistaken for the other.
- Existing PostgreSQL tests verify operation/intent/attempt/event commit and rollback, job submission idempotency, lease expiry/reclaim, and stale-token rejection. A PostgreSQL-backed process-interruption/restart test through `ExecuteAsyncProviderDelivery`, the queue, worker leases, and transaction composition now exists (Backend Tests run [#499](https://github.com/palestiny/ai-video-factory/actions/runs/37870360710)). Remaining work is broader concurrent verification of terminal, stale-generation, and cancellation invariants plus deployment startup configuration.
- Verify all terminal/stale-generation/cancellation paths under concurrent PostgreSQL workers, including crash after durable poll scheduling but before source ACK, and ensure a restarted worker recovers without duplicate provider submission or a lost poll.
- The queue port still exposes `enqueue_after(job_id, delay)` for generic execution work; provider polls correctly use `WorkIntentRepository` inside the operation-state transaction. Webhook-triggered immediate checks must eventually use the same durable intent contract rather than a separate scheduling path.
- **Durable request-input gap before production runner wiring:** `generation_jobs` persists identity/status but not the original generation inputs, while `ExecuteAsyncProviderDelivery.handle` receives `inputs` from its caller. A restart before a provider operation is durably recorded cannot reconstruct a queued request from `job_id` alone. Before adding a long-running entrypoint, resolve [Architecture Gate 008 — Durable Request Payload and Worker Rehydration](ARCHITECTURE_GATE_008_DURABLE_REQUEST_PAYLOAD.md), including references/constraints, size limits, sensitive-data handling, and retention. No payload column or storage strategy is being selected implicitly here.
- Operational readiness still requires observability for queue age, delivery attempts, expired/reclaimed leases, poll retries, dead-letter state, and recovery outcomes.

## Acceptance criteria

Gate 006 remains **NOT PASSED** until the runtime entrypoint wires the configured connection factory and remaining terminal/stale-delivery invariants are verified under concurrent PostgreSQL workers. Repository-level atomicity, lease fencing, and the async-worker crash/restart integration test now pass, but the remaining invariants still need explicit evidence.


- Backend Tests run [#603](https://github.com/palestiny/ai-video-factory/actions/runs/37915421934) passed on the current branch commit `411ca06f71d3be58ed5bf15a404e535dcc72de2c` with **174 passed**. This run includes the composed PostgreSQL persistence transaction, commit/rollback atomicity across provider-operation state, poll intents, attempt history and events, idempotency reservation, worker-lease fencing/revalidation, and the existing crash/restart recovery integration coverage. Gate 006 remains **NOT PASSED**: broad concurrent terminal/stale-generation/cancellation verification and production runtime/deployment readiness remain open.


- Backend Tests run [#607](https://github.com/palestiny/ai-video-factory/actions/runs/37916239884) passed on commit `26d931ac3d6ef8d21ad94b107eeaeef9f520a082` with **175 passed**. Added a real-PostgreSQL concurrency test proving two simultaneous reservations for the same idempotency key and canonical fingerprint resolve to one durable job: exactly one `CREATED`, one `EXISTING`, and no duplicate job row. This strengthens submission concurrency evidence; Gate 006 remains **NOT PASSED** pending broader concurrent terminal/stale-generation/cancellation verification and runtime/deployment readiness.

- Backend Tests run [#615](https://github.com/palestiny/ai-video-factory/actions/runs/37925536606) passed on commit `479574c7ce6618ce206a2c808bf8e7cfcb92eb4b` with **176 passed**. Added a read-only PostgreSQL queue health snapshot reporting pending/due-pending work, active and expired claims, dead-letter count, oldest unresolved age, oldest due-pending age, and maximum delivery attempt. The integration test validates metric deltas against real PostgreSQL rows without assuming an empty shared test database. This improves operational visibility but is not a metrics exporter, alerting policy, or production health endpoint; Gate 006 remains **NOT PASSED** pending the remaining runtime/deployment and lifecycle verification work.


## Shared PostgreSQL transaction adapter — 2026-10-09

The branch now contains `PostgresPersistenceTransaction`, composing the job, attempt-history, event, idempotency, provider-operation, and work-intent adapters over one caller-owned psycopg connection. Repository adapters do not independently commit. The transaction owner controls commit/rollback, and a lease checked during execution is revalidated under a row lock at commit time so an expired/replaced lease rejects the staged writes.

Integration coverage has been added for:
- committing provider-operation state, poll intent, attempt history, and event together;
- rolling those writes back together;
- active, mismatched, and expired worker-lease rejection, including lease expiry before commit;
- reserving an idempotency key before inserting its job in the same transaction;
- concurrent same-key submissions resolving to one durable job.

**Evidence status:** implementation and tests are present in the branch; the CI run for this adapter/test revision must be inspected before treating these behaviors as verified. Gate 006 remains **NOT PASSED** until the PostgreSQL integration suite is green and crash/restart recovery is demonstrated end-to-end.
