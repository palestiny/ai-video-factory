# Architecture Gate 003 — Provider Adapter Lifecycle and Technology Selection

## Status
Decision target: approve the provider-adapter lifecycle and acceptance boundary before selecting or implementing a concrete vendor adapter.

This gate does not select a vendor.

## Verified facts
- GenerationRequest.idempotency_key is required and stable across redelivery/retry.
- GenerationResult already normalizes provider, provider operation ID, artifacts, usage, cost, and diagnostics.
- Worker execution already distinguishes ambiguous external outcomes from ordinary retryable failures.
- Worker recovery already supports IDEMPOTENT, RECONCILABLE, and NON_RECONCILABLE.
- The current application provider ports expose a single synchronous generate(request) -> GenerationResult operation.
- CI run #216 passed for commit 63103560b4de01ede7c715495cefaa504e1923d8.

## Architectural gap
The current generate() port is sufficient for a deterministic synchronous provider test double, but it does not explicitly model providers whose lifecycle is submit -> accepted -> polling/webhook -> terminal.

That gap affects provider operation identity, cancellation, long-running lease handling, process restart, and recovery after polling interruption.

## Options
### A — Blocking adapter facade
Keep generate(request) -> result and let infrastructure submit, poll, and wait internally.
Pros: smallest surface and fastest MVP integration.
Cons: long worker occupancy, weak cancellation, hidden provider state, harder restart/recovery semantics.
Assessment: acceptable only for a deliberately synchronous/small MVP provider; not preferred long-term.

### B — Operation-oriented adapter
Model submit, status, cancellation, and reconciliation explicitly around a provider operation.
Pros: naturally models asynchronous video generation; provider operation ID is first-class; cancellation and recovery are explicit; polling and webhooks can converge.
Cons: larger contract and more durable state/test surface.
Assessment: RECOMMENDED production boundary.

### C — Hybrid
Keep generate() as the facade while allowing internal asynchronous implementations.
Pros: minimal compatibility impact.
Cons: async lifecycle remains hidden and adapter behavior becomes operationally inconsistent.
Assessment: temporary compatibility path only.

## Decision recommendation
Adopt Option B — operation-oriented provider lifecycle as the production target.

The existing synchronous generate() port may remain temporarily useful for an MVP compatibility adapter, but no concrete production provider should be approved against a boundary that cannot represent its actual lifecycle safely.

## Required normalized concepts
- ProviderOperation: provider, operation ID, logical idempotency key, capability, submitted timestamp.
- ProviderOperationStatus: submitted/running/succeeded/failed/cancelled/unknown, normalized failure, artifacts, usage/cost.
- CancellationResult: accepted, already terminal, unsupported, or unknown.
- Reconciliation result: found, not found, or unknown/error.

Exact Python names remain provisional until contract tests are written.

## Timeout and lease decision
Separate submission timeout, polling timeout, and local operation-lifetime timeout.

A long-running provider operation should not require one worker lease to remain held for its whole lifetime. The preferred production design persists the provider operation identity and re-enters execution through durable work.

## Webhook versus polling
Polling and webhooks are infrastructure mechanisms, not domain concepts.

The normalized lifecycle must support polling-only providers and webhook-capable providers. Webhooks are at-least-once delivery and must converge through the same durable operation identity and local state.

Recommendation: support polling first unless the selected provider requires webhooks for reliable completion.

## Cancellation
Cancellation is best-effort and capability-based. Local cancellation remains authoritative. Provider cancellation may race with completion and an unknown cancellation result requires reconciliation.

## Artifact and cost boundaries
Provider completion returns stable artifact references. Downloading, transcoding, and object-storage persistence belong to the later asset/storage boundary.

Provider-reported cost and usage are normalized observations; missing cost does not silently become generation failure.

## Contract-test gate
A concrete adapter must prove stable identity, duplicate submission behavior, submission/polling ambiguity, terminal success/failure, cancellation races, reconciliation found/not-found/unknown, operation ID preservation, artifact/usage/cost normalization, repeated polling/webhook harmlessness, and no vendor types crossing the application boundary.

Tests must prove the declared operation-safety mode rather than trusting configuration.

## Non-decisions
This gate does not choose the video/image/voice/LLM vendor, HTTP client, SDK, queue, database, storage, webhook transport, or deployment platform.

## Gate result
PASS recommendation / implementation prerequisite: operation-oriented provider lifecycle is the preferred production boundary.

Implementation slice now added: durable operation repository state is wired into the execution persistence boundary, and an application lifecycle service covers submit -> durable persistence, status polling -> durable status, and cancellation semantics. Deterministic tests cover identity reuse, status progression, repeated polling, cancellation races, and ambiguous submission persistence.

**Verification status: PENDING CI.** Concrete provider implementation remains blocked until the lifecycle contract/test slice is green.

## Next implementation slice
1. Add provider operation lifecycle contracts.
2. Add deterministic contract tests for synchronous and asynchronous provider behavior.
3. Define polling/webhook convergence and durable operation state.
4. Re-run Gate 003.
5. Only then evaluate concrete provider capabilities, cost, latency, quality, and API constraints.