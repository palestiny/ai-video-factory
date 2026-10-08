# Architecture Gate 005 — Webhook/Poll Convergence

**Status: DESIGN PROPOSAL — not implemented; provider-neutral contract only.**

## Goal

Allow asynchronous providers to notify the application through webhooks without creating a second completion path that can race with polling, duplicate terminal events, or resurrect cancelled jobs.

## Current verified baseline

- Gate 004 persists provider operation identity/status and finalizes job/attempt state through the async worker.
- Queue deliveries are at-least-once hints; durable job and provider-operation records are authoritative.
- Local cancellation is authoritative: a late provider success must not move a cancelled job back to success.
- No concrete provider, webhook endpoint, persistence adapter, or broker has been selected.

## Proposed invariants

1. **One canonical operation identity.** Resolve notifications using the persisted pair `(provider_name, provider_operation_id)`; never trust a job ID supplied only by an unverified payload.
2. **One canonical transition path.** Webhook handling must not write job/attempt terminal state independently. It must produce a normalized operation observation or wake-up signal consumed by the same application lifecycle used by polling.
3. **Duplicate-safe delivery.** Repeated webhook events and queue deliveries must not create duplicate logical terminal transitions or duplicate job events.
4. **Out-of-order safe.** An older non-terminal observation cannot regress a persisted terminal provider operation. Where the provider supplies a trustworthy sequence/version, use it; otherwise treat webhook delivery as a wake-up hint and fetch current status.
5. **Cancellation wins locally.** Provider success received after local cancellation may be retained as provider-operation evidence, but must never resurrect the logical job or overwrite the cancellation event.
6. **Atomic local state.** Persist deduplication identity, accepted operation observation, and any resulting local transition in a transaction with concurrency protection. The concrete transaction/unique-key implementation remains a production-adapter gate.
7. **Authentication before processing.** Verify provider signature/authentication, timestamp/replay window where supported, and payload size before accepting an event. Never log secrets or raw sensitive payloads.

## Recommended baseline

**Default: webhook as a wake-up hint, poll as the normalized status read.**

1. Verify the request against the provider-specific webhook security contract.
2. Validate the minimum envelope and correlate it to a previously persisted provider operation.
3. Record a provider event/delivery ID under a unique key when the provider supplies a stable ID. If no stable event ID exists, use a documented bounded deduplication strategy; do not assume payload hashing alone proves uniqueness.
4. Enqueue an immediate status-check intent for the canonical operation identity.
5. The normal async worker obtains current provider status and applies it through `ProviderOperationLifecycle`.
6. If enqueue fails, preserve the durable intent and retry through the selected outbox/durable-queue strategy. Do not return success to the provider until the event has been durably accepted or the provider's retry contract makes a different acknowledgement policy explicit.

This keeps provider-specific webhook payloads outside the domain and avoids trusting potentially stale or partial callback payloads as the final result.

## Conditional optimization

A provider may later support direct terminal-observation ingestion only if its adapter contract proves:
- webhook authenticity and event identity;
- operation ID and event-to-operation correlation;
- event ordering/version semantics or a safe stale-event rule;
- normalized terminal result/failure payload completeness;
- duplicate delivery behavior;
- race handling against poll responses and cancellation;
- transactional deduplication and operation/job transition semantics.

Without those proofs, callbacks remain wake-up hints.

## Poll/webhook race rules

- If polling and webhook processing both request a status check, coalesce by operation identity where practical; correctness must not depend on coalescing.
- If two workers race, persistence must use a transaction/optimistic concurrency rule so only a valid monotonic transition commits.
- A terminal provider operation is not downgraded by a later non-terminal observation.
- Job terminalization is separately guarded by current durable job state. A cancelled job remains cancelled even if the provider operation later reports success.
- A database commit failure means the local transition is not acknowledged as complete; retry/reconciliation must reuse the same operation identity.

## Proposed application ports (names are illustrative, not API decisions)

- `ProviderWebhookVerifier`: validates signature/authenticity and returns a verified normalized envelope.
- `ProviderNotificationDeduplicator`: atomically reserves a stable provider event ID where available.
- `ProviderOperationWakeup`: requests an immediate check for a persisted provider operation.
- Existing `ProviderOperationLifecycle`: remains the canonical status persistence/transition path.

Do not add these ports to production code until the persistence and delivery contracts are specified and tests can prove atomicity.

## Required tests before implementation approval

1. invalid signature/payload is rejected before state mutation;
2. duplicate webhook event does not create duplicate dispatch intent or terminal event;
3. unknown operation ID is rejected or quarantined without creating a job;
4. webhook arriving before the first poll schedules an immediate status check;
5. poll and webhook concurrently observe success but only one logical terminal transition/event is committed;
6. stale non-terminal webhook cannot regress a terminal operation;
7. webhook success racing with local cancellation never resurrects the job;
8. enqueue/dispatch failure is recovered without losing the accepted notification;
9. persistence commit failure followed by retry is idempotent;
10. missing provider sequence/event ID follows a documented conservative fallback.

## Open decisions for a later technology gate

- concrete webhook transport and signature mechanism per provider;
- whether each selected provider offers stable event IDs, event versions, and complete terminal payloads;
- event inbox/outbox schema and uniqueness constraints;
- durable queue/broker or database-backed scheduling;
- retention, replay window, rate limits, dead-letter/quarantine, and operational replay procedure;
- provider-specific integration tests and secrets configuration.

## Acceptance gate

Gate 005 is **not passed** until the proposed invariants are implemented and tested against the chosen production persistence and delivery adapters. This document is a design proposal and does not select a vendor, database, queue, or direct-webhook-finalization strategy.
