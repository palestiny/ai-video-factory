# Architecture Decisions

## ADR-001 — Provider-neutral core

**Decision:** vendor integrations are adapters behind domain/application ports.

**Reason:** providers change quickly; business workflow should remain stable.

## ADR-002 — Durable logical jobs and immutable attempts

**Decision:** GenerationJob represents logical work; GenerationAttempt represents each execution.

**Reason:** enables retries, auditability, cost attribution, and recovery without duplicating logical work.

## ADR-003 — Deterministic fake providers

**Decision:** every capability gets deterministic fakes for automated tests.

**Reason:** architecture must be testable without paid external APIs.

## ADR-004 — MVP excludes publishing and analytics

**Decision:** production pipeline is the first milestone.

**Reason:** validate the core production engine before adding distribution and feedback loops.


## ADR-005 — Provider recovery safety is explicit

**Decision:** provider adapters must declare whether repeated logical operations are idempotent, reconcilable, or non-reconcilable. Ambiguous external outcomes must not be treated as ordinary retryable failures by default.

**Reason:** a worker crash or persistence failure can leave a billable provider operation existing even when local completion is unknown. Explicit recovery semantics prevent the orchestration layer from assuming duplicate execution is safe.


## ADR-006 — Ambiguous provider outcomes fail closed

**Decision:** an ambiguous provider outcome is handled at the worker delivery boundary, not normalized into an ordinary provider failure. Idempotent providers may be safely redelivered with the same key; reconcilable providers must be reconciled before resubmission; non-reconcilable providers become terminal RECONCILIATION_REQUIRED until an explicit operational recovery path exists.

**Reason:** a timeout, process crash, or persistence failure can leave an external generation billable while local state is uncertain. The orchestration layer must preserve that uncertainty rather than convert it into a blind retry.
