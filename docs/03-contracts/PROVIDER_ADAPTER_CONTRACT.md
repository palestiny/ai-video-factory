# Concrete Provider Adapter Contract

## Status

**Decision target:** define the production adapter boundary before selecting a concrete provider.

This contract does not select Veo, Runway, ElevenLabs, a specific image provider, or any other vendor.

## Purpose

A provider adapter translates an external provider API into the application's provider-neutral generation contract without leaking vendor types, failure semantics, or lifecycle assumptions into the domain/application model.

The adapter is responsible for:

- translating a normalized GenerationRequest
- applying provider-specific authentication/configuration
- enforcing provider timeout behavior
- forwarding stable operation identity when supported
- exposing whether the provider is idempotent, reconcilable, or non-reconcilable
- translating provider responses into GenerationResult
- translating provider failures into normalized Failure
- exposing provider operation identity
- reporting usage/cost metadata when available
- exposing cancellation capability without making cancellation a domain requirement

## Boundary

Application:

GenerationRequest -> GenerationProvider -> GenerationResult | normalized failure

Infrastructure:

provider-neutral request -> vendor request -> vendor API -> vendor response -> provider-neutral result

Vendor SDK classes, HTTP response objects, vendor status enums, and vendor-specific exceptions must not cross the application boundary.

## Required provider declaration

Every production adapter must expose:

- provider name
- supported capability
- operation-safety mode
- timeout policy
- cancellation capability
- reconciliation capability when applicable

The operation-safety declaration must use the existing ProviderExecutionContract(provider, operation_safety) with:

- IDEMPOTENT
- RECONCILABLE
- NON_RECONCILABLE

An adapter must not claim IDEMPOTENT merely because the provider accepts a client-generated request ID. Contract tests must establish that reuse of the same identity cannot create a second logical billable operation.

## Operation identity

GenerationRequest.idempotency_key is the application's stable logical operation identity.

The adapter must document:

1. whether the provider receives this identity directly;
2. how the identity maps to provider fields;
3. whether provider retries reuse the same identity;
4. what happens if the provider accepts an operation but the response is lost;
5. how the adapter distinguishes duplicate/replayed submission from a new operation.

The adapter must never generate a new logical operation identity for queue redelivery.

## Ambiguous outcomes

Provider timeout, connection loss, process interruption, or an uncertain provider response must not automatically become an ordinary retryable failure.

The adapter must classify an outcome as ambiguous when it cannot establish whether the external operation exists.

For an ambiguous outcome:

- IDEMPOTENT: raise AmbiguousProviderOutcome and allow worker redelivery with the same stable key.
- RECONCILABLE: raise AmbiguousProviderOutcome with reconciliation support.
- NON_RECONCILABLE: raise AmbiguousProviderOutcome and let the worker persist RECONCILIATION_REQUIRED.

The adapter must not hide ambiguity by returning a fabricated GenerationResult or ordinary PROVIDER_FAILURE.

## Timeout semantics

Timeouts must distinguish at least:

- client-side request timeout before provider acceptance is known;
- provider-reported operation timeout;
- asynchronous generation still running after submission;
- polling timeout where the final provider state remains unknown.

A timeout is not automatically evidence that no billable provider operation exists.

The adapter must document which timeout class can produce ambiguity.

## Cancellation

Cancellation is capability-based.

The application may request cancellation of the logical generation job. If the provider supports cancellation, the adapter may expose it.

Provider cancellation must define:

- whether cancellation is synchronous or asynchronous;
- whether a cancellation request can race with completion;
- whether cancellation is billable;
- what happens when the provider returns "already completed";
- how a late completion is reconciled with local job state.

A provider that does not support cancellation remains valid; the worker must still prevent cancelled local jobs from being newly submitted.

## Result normalization

A successful adapter call must return GenerationResult containing, where available:

- provider name
- provider operation ID
- artifact references
- usage metadata
- cost metadata
- provider-neutral diagnostics

The adapter must preserve provider operation identity whenever the provider exposes one. It must not encode vendor-specific response objects into artifact_refs, diagnostics, or application-visible structures.

## Failure normalization

Provider failures must map to the existing normalized FailureCode taxonomy.

At minimum, the adapter must distinguish where evidence permits:

- invalid request
- authentication
- rate limiting
- timeout
- content rejection
- transient network failure
- provider failure
- ambiguous external outcome

Provider-specific error codes may remain in diagnostics, but application retry behavior must be driven by normalized semantics.

## Cost and usage

Cost/usage metadata is observational unless the provider contract explicitly requires it for correctness.

The adapter should report:

- input units
- output units
- duration where applicable
- provider-reported cost
- currency if provided
- provider operation ID

Missing cost data must not cause a successful generation to become a failed generation unless the product explicitly declares cost accounting mandatory for that provider.

## Contract tests

Every production adapter must pass deterministic contract tests covering:

1. successful normalized result;
2. invalid request normalization;
3. authentication failure normalization;
4. rate-limit normalization;
5. timeout classification;
6. transient network failure;
7. content rejection where supported;
8. stable operation identity;
9. duplicate submission behavior;
10. ambiguous outcome propagation;
11. reconciliation found;
12. reconciliation not found;
13. reconciliation lookup error when applicable;
14. provider operation ID preservation;
15. usage/cost normalization;
16. cancellation behavior when supported;
17. provider-specific payloads do not cross the application boundary.

Tests must prove the declared ProviderOperationSafety mode rather than trusting configuration alone.

## Acceptance gate

A provider adapter may be approved for production only when:

- its capability is explicitly declared;
- its operation-safety mode is proven by contract tests;
- ambiguous outcomes are explicit;
- timeout semantics are documented;
- stable operation identity is proven;
- reconciliation is implemented and tested when required;
- normalized result/failure contracts pass;
- operation identity is preserved;
- usage/cost behavior is documented;
- cancellation semantics are documented or explicitly unsupported;
- no vendor-specific types cross the application boundary.

## Non-decisions

This contract intentionally does not choose:

- video provider
- image provider
- voice provider
- LLM provider
- HTTP client
- SDK
- queue
- database
- secrets manager
- deployment platform

Those choices belong to a later technology decision gate after adapter semantics are accepted.
