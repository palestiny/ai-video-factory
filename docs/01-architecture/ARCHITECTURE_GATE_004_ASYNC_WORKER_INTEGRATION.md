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

CI run #272 passed on code/test commit `fe77696694d0e0c64cb2c913f449ef44f5594934`. CI run #278 passed on code/test commit `2341d721664dd00ba2e4e0d932be87eb2b27697b`.

Integration tests cover:
- non-terminal poll persists state, schedules a later delivery, acknowledges current delivery, and releases the lease;
- terminal success completes the job and attempt on a later delivery;
- job-finalization commit failure followed by redelivery completes from the durable terminal operation without another provider submission or poll;
- ambiguous non-reconcilable submission is terminalized as `RECONCILIATION_REQUIRED`;
- reconcilable ambiguity found-result completes without resubmission;
- explicit reconciliation not-found permits a submission attempt, while lookup failure requeues without resubmission;
- queue scheduling failure requeues the delivery without losing the durable provider operation.

## Remaining gaps — do not mark production-ready

1. Add cancellation-race tests, including cancellation after provider success is observed but before job finalization; define how the active attempt is terminalized when its job is cancelled.
2. Define production outbox/transaction strategy so a durable non-terminal operation cannot be stranded if scheduling fails.
3. Add provider-specific contract tests for submit/status identity, terminal result normalization, and operation-safety guarantees.
4. Verify the persistence implementation's transaction isolation, optimistic concurrency, and atomic lookup of operation/attempt history. The current implementation is validated against in-memory test doubles only.
5. Make polling and webhook notifications converge on the same operation identity and terminal transition path.
6. Define polling limits, backoff, timeout, and dead-letter/manual-reconciliation policy.

## Decisions

- Keep the existing synchronous delivery service intact; async operations use a distinct worker application service.
- Keep provider and infrastructure vendor choices deferred.
- Do not claim production readiness until the remaining gaps are covered by tests and a real persistence/queue adapter.

## Next

Resolve cancellation/attempt lifecycle semantics, then review production transaction/outbox semantics before selecting a concrete provider.
