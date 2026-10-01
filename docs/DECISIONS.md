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
