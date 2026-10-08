# Architecture Gate 006 — Durable Poll Dispatch and Outbox Contract

**Status: DESIGN PROPOSAL — no production persistence or queue implementation selected.**

## Goal

Specify the correctness contract for scheduling future provider-status checks so that a durable non-terminal operation cannot be stranded between a database commit and queue publication.

This gate defines required behavior, not a database schema or broker choice.

## Failure window being closed

With direct enqueue followed by acknowledgement, a worker can:
- persist the provider operation as non-terminal;
- publish a future poll message;
- crash before acknowledging the current delivery.

This can create duplicate messages. More dangerously, a future refactor that acknowledges before durable scheduling could lose the poll entirely. The production design must tolerate duplicates and make accepted scheduling intent durable before acknowledging the source delivery.

## Required invariants

1. **Atomic intent creation.** When a status observation requires another poll, durable operation state and the next poll intent are committed in one local database transaction.
2. **Stable intent identity.** A logical poll generation has one stable key, proposed as `(provider_name, provider_operation_id, poll_generation)`. Repeated handling must not create unbounded new intents for the same generation.
3. **At-least-once dispatch.** The dispatcher may publish the same intent more than once if broker confirmation and local marking are separated by a crash. Consumers must remain idempotent.
4. **No premature acknowledgement.** The worker may acknowledge its source delivery only after the operation observation and required next-poll intent are durably committed.
5. **Terminal suppression.** A terminal provider operation must not create new poll generations. Previously queued poll messages for terminal/cancelled jobs must safely no-op or reconcile durable state.
6. **Stale generation protection.** A delayed message from poll generation N must not supersede a newer accepted observation or create generation N+1 more than once.
7. **Concurrency protection.** Concurrent workers must not commit duplicate logical poll generations. Enforce this with a unique constraint and transactional/optimistic conflict handling in the production adapter.
8. **Bounded retry policy.** Dispatch failures retry with bounded backoff and observable attempt counts; persistent failures become alertable and operationally recoverable rather than silently abandoned.
9. **No provider call in dispatcher.** The outbox dispatcher publishes delivery intent only. It does not poll providers, finalize jobs, or own provider lifecycle rules.
10. **Cancellation safety.** Cancellation does not require erasing already-published messages. Workers re-read durable job/operation state and must not resurrect a cancelled job.

## Proposed logical records (illustrative only)

### PollDispatchIntent

- stable intent ID / unique key;
- provider name and provider operation ID;
- logical job ID;
- poll generation;
- not-before timestamp;
- creation timestamp;
- dispatch status (pending / delivered, with failure represented by attempt metadata rather than a fake terminal success);
- dispatch attempt count;
- last dispatch error code and timestamp, with sensitive data excluded.

### Dispatcher lease/claim

A dispatcher claims due pending intents using an atomic claim/lease mechanism. Lease expiry allows recovery after a dispatcher crash. The implementation must avoid two dispatchers permanently owning the same intent and must define behavior when broker publish succeeds but the dispatcher crashes before marking the intent delivered.

These names and fields are a proposal, not a committed schema.

## Proposed flow

1. Worker reads current durable job and provider operation.
2. Provider status is normalized and persisted.
3. If status is non-terminal and polling is still permitted, the same transaction inserts or reuses the next unique poll intent with its not-before time.
4. Transaction commits.
5. Worker acknowledges the source queue delivery.
6. Dispatcher claims due intent and publishes a message carrying the stable intent/operation identity.
7. After broker confirmation, dispatcher marks the intent delivered.
8. If dispatcher crashes after publish and before marking delivered, it republishes after lease expiry; duplicate message is expected and harmless.
9. Consumer claims the job lease, reads durable state, and treats the message as a hint. It checks generation/state before calling the provider.
10. Terminal status prevents creation of another poll intent.

## Retry and operational behavior

- Provider-status errors and queue-dispatch errors are different failure classes with separate counters and policies.
- A transient broker error retains the intent as pending and schedules retry.
- Repeated dispatch failure raises an alert based on oldest pending age and retry count.
- A dead-letter/quarantine state must preserve the intent and failure metadata for operator replay; it must not silently delete the only recovery path.
- Poll timeout/max-attempt policy belongs to the provider-operation lifecycle policy, not the dispatcher.
- Intent retention and cleanup must preserve audit/recovery requirements.

## Required tests

1. operation-state commit and intent creation are atomic;
2. rollback leaves neither a partially advanced operation nor a poll intent;
3. duplicate insertion for the same poll generation resolves to one logical intent;
4. concurrent workers cannot create two intents for the same generation;
5. publish succeeds then dispatcher crashes before mark-delivered; redelivery duplicates safely;
6. publish fails; intent remains pending and retries;
7. dispatcher lease expires after crash and another dispatcher recovers the intent;
8. terminal operation cannot create another poll intent;
9. stale poll-generation message cannot regress operation state;
10. cancelled job remains cancelled when stale poll message arrives;
11. retry exhaustion is observable and preserves recoverable intent metadata;
12. tests run against the real selected database/queue adapters, not only in-memory fakes.

## Options still open

- Transactional outbox in the relational database;
- database-backed delayed queue;
- a managed workflow/queue product with documented durable scheduling guarantees.

Choose only after comparing transaction support, delayed delivery, operational burden, throughput, cost, local-development fit, and portability. Do not choose a vendor merely because this contract uses the word “outbox.”

## Acceptance criteria

Gate 006 is not passed until:
- a concrete persistence/queue design is approved;
- unique identity and transaction semantics are documented;
- crash-window and concurrency tests pass against real adapters;
- metrics and recovery procedures are defined;
- webhook-triggered immediate checks use the same durable dispatch contract;
- the project demonstrates that no non-terminal operation can be stranded solely by a dispatch crash.

