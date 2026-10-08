# Architecture Gate 007 — Persistence and Durable Scheduling Decision

**Status: APPROVED — Option A selected by project owner on 2026-10-09.**

## Decision

Adopt **PostgreSQL as the initial durable persistence target** and a **PostgreSQL-backed delayed work queue** for the first production-oriented implementation.

The owner explicitly approved Option A. This selects the persistence/scheduling architecture, not a hosting provider, managed database vendor, or production deployment plan.

### Why this is the MVP choice

- Keep job state, attempt history, provider operation identity, lifecycle events, and due work in one transactional system.
- Make work intent durable in the same transaction as the state transition that requires it.
- Avoid operating a separate broker before measured throughput or broker-specific capabilities justify it.
- Preserve application/domain ports so a broker can be introduced later without changing domain rules.

### Required invariants

1. Job state, attempt history, provider-operation state, events, and new work intent commit atomically when part of one application transition.
2. A work item has a stable, unique intent key; retries/redeliveries must not create a second logical intent.
3. Work delivery is at-least-once. Consumers must be idempotent.
4. Claiming due work must be concurrency-safe; use PostgreSQL row locking such as FOR UPDATE SKIP LOCKED or an equivalently proven atomic claim strategy.
5. Expired claims must be recoverable; every claim uses a fresh token and ACK must validate current ownership.
6. Cancellation and terminal state are authoritative; stale work must not resurrect a job or regress a provider operation.
7. All timestamps are timezone-aware UTC instants at the application boundary and TIMESTAMPTZ in PostgreSQL.
8. Schema migrations and adapter contract tests are required. In-memory tests alone cannot pass the production persistence gate.

## First schema baseline

`backend/migrations/0001_postgresql_durable_work.sql` adds initial tables for jobs, idempotency reservations, append-only attempt versions, provider operations, lifecycle events, and delayed work items, plus primary/unique constraints and due-work indexes.

**Important:** the migration is a schema baseline only. It is not yet a tested production adapter and does not establish the gate as passed. Migration execution, domain serialization, transaction implementation, lease fencing, database-backed queue behavior, and real PostgreSQL concurrency/crash-window tests remain required.

## Deferred until evidence justifies it

- A separate message broker and transactional outbox dispatcher.
- Selection of a cloud host or managed PostgreSQL vendor.
- Final workload-specific index tuning, retention, and backup/restore configuration.

## Acceptance evidence required

- Apply migrations against the supported PostgreSQL version in CI.
- Test transaction rollback across job/attempt/operation/event/work-intent writes.
- Test two concurrent workers claiming the same due item; only one may own it.
- Test lease expiry/reclaim, stale-token ACK rejection, duplicate intent insertion, and process restart recovery.
- Test commit-before-ACK redelivery, terminal replay, cancellation races, and poll-generation deduplication against PostgreSQL.
- Demonstrate recovery for old pending work and expired claims; expose queue age and retry metrics.

Option A is approved. Production readiness is **NOT PROVEN** until these tests and operational controls exist.
