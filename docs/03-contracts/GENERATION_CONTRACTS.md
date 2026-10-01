# Generation Contracts

## Normalized request

Every generation capability receives a domain-neutral request containing:
- request/job identity
- capability
- normalized inputs
- references
- execution constraints
- idempotency key

## Normalized result

Every adapter returns:
- provider
- provider operation id
- status
- output artifact references
- usage metadata
- normalized cost if known
- provider-neutral diagnostics

## Error categories

- INVALID_REQUEST — caller input cannot be executed.
- AUTHENTICATION — provider credentials rejected.
- RATE_LIMITED — provider asks caller to slow down.
- TIMEOUT — operation exceeded configured deadline.
- PROVIDER_FAILURE — provider failed execution.
- CONTENT_REJECTED — provider policy/content validation rejected input.
- TRANSIENT_NETWORK — transport failure that may be retried.
- UNKNOWN — unclassified failure requiring investigation.

Retryability is policy-driven; it is not inferred solely from an exception class.
