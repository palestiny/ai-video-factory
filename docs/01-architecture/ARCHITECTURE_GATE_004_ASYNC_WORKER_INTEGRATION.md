# Architecture Gate 004 — Asynchronous Provider Worker Integration

**Status: PASS for the deterministic application slice; NOT PROVEN for production integration.**

## Goal

Connect the operation-oriented provider lifecycle from Gate 003 to worker deliveries without holding a worker lease for the full external generation runtime. Keep the existing synchronous worker path unchanged for compatibility.

## Implemented behavior

- Added `ExecuteAsyncProviderDelivery` as a separate application service.
- Each delivery claims a short lease, re-reads the durable job, and resolves the provider operation by the provider name plus the job's stable idempotency key.
- The first delivery records the running job, attempt, and start event before submitting the external operation.
- Provider operation identity and each observed status are persisted through `ProviderOperationLifecycle`.
- A non-terminal status schedules a later delivery and acknowledges the current delivery only after the operation status is durable.
- Terminal provider success/failure is applied to the job and active attempt in a separate transaction.
- If terminal job finalization fails after the provider operation is durably terminal, redelivery reuses the stored operation outcome and does not poll or submit again.
- A late provider result is not allowed to turn a job already observed as cancelled into success.
- An ambiguous operation for a non-reconcilable provider is recorded as `RECONCILIATION_REQUIRED`, rather than blindly submitting again.
- For a reconcilable provider with no durable operation identity, resubmission is blocked unless the adapter's reconciliation port explicitly reports that no operation exists. A missing reconciliation port or lookup error requeues without blind resubmission.

## Delivery/lease invariant

The lease covers one submit/poll/finalize delivery only. A non-terminal operation schedules a later delivery; the worker does not retain the lease while the provider runs. Queue messages remain delivery hints; durable job and provider-operation state are authoritative.

## Verified

- CI run #272 passed on code/test commit `fe77696694d0e0c64cb2c913f449ef44f5594934`.
- CI run #278 passed on code/test commit `2341d721664dd00ba2e4e0d932be87eb2b27697b`.
- CI run #289 passed on cancellation test commit `4c4134573b54f322d7ee24abb5ebc9c0c95f2eef`: [Backend Tests](https://github.com/palestiny/ai-video-factory/actions/runs/37852174052).
- CI run #297 passed on the documentation update commit `7640f19637afd3d5cea866deec28ff13474ca7d5`: [Backend Tests](https://github.com/palestiny/ai-video-factory/actions/runs/37852540644).

Integration tests cover:
- non-terminal poll persists state, schedules a later delivery, acknowledges current delivery, and releases the lease;
- terminal success completes the job and attempt on a later delivery;
- job-finalization commit failure followed by redelivery completes from the durable terminal operation without another provider submission or poll;
- ambiguous non-reconcilable submission is terminalized as `RECONCILIATION_REQUIRED`;
- reconcilable ambiguity found-result completes without resubmission;
- explicit reconciliation not-found permits a submission attempt, while lookup failure requeues without resubmission;
- queue scheduling failure requeues the delivery without losing the durable provider operation;
- cancellation after provider success is observed does not resurrect the job and terminalizes the active attempt;
- cancellation between deliveries terminalizes the active attempt without another provider poll.

## Remaining gaps — do not mark production-ready

1. Add cancellation endpoint/use-case integration tests and verify transaction isolation against the production adapter.
2. Define production outbox/transaction strategy so a durable non-terminal operation cannot be stranded if scheduling fails.
3. Add provider-specific contract tests for submit/status identity, terminal result normalization, and operation-safety guarantees.
4. Verify the persistence implementation's transaction isolation, optimistic concurrency, and atomic lookup of operation/attempt history. The current implementation is validated against in-memory test doubles only.
5. Make polling and webhook notifications converge on the same operation identity and terminal transition path. See [Gate 005](ARCHITECTURE_GATE_005_WEBHOOK_POLL_CONVERGENCE.md).
6. Define polling limits, backoff, timeout, and dead-letter/manual-reconciliation policy.

## Transaction/outbox assessment — proposal, not yet selected

### Current evidence and failure window

The application persists provider-operation state before it schedules the next poll, and only acknowledges the current queue delivery after scheduling succeeds. This is safe against a simple scheduling exception when the broker reliably redelivers unacknowledged messages. It is not an atomic database/queue commit: if scheduling succeeds but ACK fails, duplicate poll messages are possible; correctness therefore still depends on idempotent delivery handling and lease/concurrency controls.

### Options

1. **Direct enqueue + ACK ordering (current deterministic slice).** Smallest design; depends on at-least-once broker redelivery, idempotent handlers, and lease protection. Does not eliminate duplicate scheduling after the enqueue/ACK crash window.
2. **Transactional outbox (recommended for production).** In the same database transaction that persists a non-terminal provider operation, insert a uniquely keyed poll-dispatch intent. A dispatcher publishes pending intents and marks them delivered only after broker confirmation. Use a uniqueness key such as `(job_id, provider_operation_id, next_poll_generation)` so retries do not create unbounded duplicate intents. Delivery remains at-least-once, so consumers must still be idempotent.
3. **Database-backed queue.** Store scheduled poll work in the same transactional datastore and let workers claim due rows. This can remove the cross-system publish gap, but couples queue throughput/retention/locking to the database and requires careful claim/lease indexing.

### Recommendation

Prefer **transactional outbox** if production uses a separate broker and relational database. Keep the outbox record in the same transaction as the durable operation state; dispatch asynchronously; tolerate duplicate delivery; and add metrics/alerts for oldest pending outbox age and repeated dispatch failures. If the chosen infrastructure provides a proven database-backed queue with suitable delayed scheduling and throughput, compare that option before committing.

This is a recommendation only. Database, broker, outbox schema, retry policy, and delivery guarantees remain unselected until the concrete persistence/queue stack is chosen. The in-memory tests do not prove this production property. The contract proposal is documented in [Gate 006](ARCHITECTURE_GATE_006_DURABLE_POLL_DISPATCH.md).

## Decisions

- Keep the existing synchronous delivery service intact; async operations use a distinct worker application service.
- Keep provider and infrastructure vendor choices deferred.
- Do not claim production readiness until the remaining gaps are covered by tests and a real persistence/queue adapter.

## Decision — cancellation and attempt lifecycle

- Job cancellation remains authoritative: a late provider success never changes a cancelled job back to success.
- Added `AttemptStatus.CANCELLED` and a cancellation factory for the attempt record. Both a late provider result and a redelivery that finds an already-cancelled job terminalize the active attempt and retain the provider operation ID when available.
- The worker does not append a second job-level `CANCELLED` event in this branch; the cancellation command owns that event. If attempt terminalization cannot commit, the delivery is requeued for retry.

## Next

Review the Gate 005 webhook/poll proposal and Gate 006 durable-dispatch contract. Then compare concrete persistence/queue options against these invariants before selecting a provider or infrastructure stack.
