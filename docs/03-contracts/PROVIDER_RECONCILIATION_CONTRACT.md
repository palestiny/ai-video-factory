# Provider Reconciliation Contract

## Problem

A provider call is an external side effect. The worker can lose the result after the provider has accepted or completed the operation, for example:

provider accepts operation -> worker loses connection/process crashes -> local completion is unknown -> redelivery arrives.

Retrying blindly can create a duplicate billable generation.

## Decision

Every production provider adapter must explicitly declare one operation-safety mode:

| Mode | Meaning | Recovery rule |
|---|---|---|
| IDEMPOTENT | Reusing the same stable operation key resolves to the same logical provider operation. | Redelivery may retry with the same key. |
| RECONCILABLE | The provider does not guarantee idempotent submission, but an existing operation can be located by stable identity. | Reconcile first; only submit again when reconciliation proves no operation exists. |
| NON_RECONCILABLE | The provider exposes neither safe idempotent submission nor reliable lookup. | Do not silently retry an ambiguous operation; surface reconciliation as an explicit operational state. |

The application contract lives in `backend/app/application/reconciliation.py`.

## Stable identity

`GenerationRequest.idempotency_key` remains stable across queue redelivery and retry. It is the logical operation identity, not a new key per delivery attempt.

For reconcilable providers, `ReconciliationQuery` uses that same identity. Provider adapters may translate it into vendor query fields, but vendor types must not cross the application boundary.

## Reconciliation port

`GenerationReconciliationPort.reconcile(query)` returns an existing normalized `GenerationResult` when the external operation is found, or `None` when it is not found.

A concrete adapter must document how lookup is scoped, what consistency guarantees exist, and what errors mean unknown versus not found.

## Ambiguous outcome rule

An ambiguous outcome is different from a normal provider failure:

- Normal retryable failure: evidence says the provider operation did not complete and retry policy may schedule another attempt.
- Ambiguous outcome: the local worker cannot establish whether the external operation exists. Recovery must consult provider idempotency or reconciliation semantics before issuing another billable operation.

The deterministic contract test suite proves that RECONCILABLE and NON_RECONCILABLE providers are not silently treated as retry-safe.

## Production adapter acceptance criteria

A provider adapter is not production-ready until it supplies:

1. declared `ProviderExecutionContract`;
2. stable operation identity behavior;
3. either native idempotency or a tested reconciliation implementation;
4. explicit handling for lookup not found versus unknown/error;
5. normalized `GenerationResult` with provider operation identity when available;
6. integration/contract tests covering duplicate submission and ambiguous recovery.

No provider-specific implementation is selected by this contract. The contract exists so provider choice remains replaceable without weakening reliability semantics.
