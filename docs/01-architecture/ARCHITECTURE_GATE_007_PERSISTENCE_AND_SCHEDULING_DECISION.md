# Architecture Gate 007 — Persistence and Durable Scheduling Decision

**Status: DECISION PROPOSAL — not approved; no infrastructure vendor selected.**

## Context verified from the repository

- The project is a provider-neutral Python platform requiring Python >=3.12.
- `backend/pyproject.toml` currently declares no runtime dependencies.
- The async worker application service and provider-operation lifecycle are exercised with in-memory test doubles.
- Gate 004 explicitly says production transaction isolation and a real persistence/queue adapter remain unproven.
- Gate 006 requires durable poll intent, duplicate-safe dispatch, concurrency protection, bounded retries, and recoverability.
- The MVP needs durable generation jobs, attempts/history, provider-operation identity, idempotency, cost tracking, and selective retries.

This gate compares architecture choices; it does not claim these infrastructure components already exist.

## Decision criteria

1. Atomic persistence of job/attempt/operation state and scheduling intent.
2. Safe behavior under process crashes, duplicate deliveries, concurrent workers, and provider delays.
3. Local Windows development and CI simplicity.
4. Operational burden and cost for an early product.
5. Migration path to higher throughput without changing domain/application contracts.
6. Testability against real adapters, including transaction isolation and recovery.
7. Portability across provider vendors.

## Persistence options

| Option | Advantages | Costs / risks | Assessment |
|---|---|---|---|
| PostgreSQL | Strong transactions, unique constraints, row-level concurrency control, suitable durable history and outbox | Requires a real database in local/CI/prod; migrations and operations must be owned | **Recommended baseline to evaluate** |
| SQLite | Very low-friction local setup; useful for narrow development scenarios | Write/concurrency behavior differs from a multi-worker production database; risks tests passing locally but failing under production concurrency | Local-only/testing option, not the production correctness target |
| Managed database selected with a cloud platform | Less database operations work | Vendor/platform coupling and deployment constraints; exact guarantees and cost depend on the service | Consider if the deployment target is chosen first |

## Durable scheduling options

| Option | Advantages | Costs / risks | Assessment |
|---|---|---|---|
| PostgreSQL-backed delayed queue | One transactional system can hold operation state and due work; no DB-to-broker publish gap | Queue polling/claiming, indexing, retention, and contention become application responsibilities | Strong simplest-MVP candidate if throughput and scheduling needs fit |
| PostgreSQL transactional outbox + separate broker | Atomic local intent; isolates domain transaction from broker choice; broker can scale independently | At-least-once publication and duplicate handling remain mandatory; more operational components | Strong production path when separate broker benefits justify its cost |
| Direct enqueue then ACK | Minimal implementation | Not atomic with DB state; crash windows can create duplicates and recovery depends on broker redelivery and idempotency | Acceptable only as a limited deterministic slice, not the production durability contract |

## Recommendation for owner review

**Keep PostgreSQL as the leading production persistence candidate, and choose scheduling separately.** Do not introduce a broker merely by habit.

Compare two viable first releases:

- **Option A — PostgreSQL + database-backed delayed work:** preferred for the smallest operational footprint if expected throughput and due-work claiming are acceptable. Operation state and poll intent can share a transaction.
- **Option B — PostgreSQL + transactional outbox + broker:** preferred if independent queue throughput, broker delay/retry features, or multiple worker pools justify another service. The outbox remains the durable source of dispatch intent; broker delivery is at-least-once.

Avoid using SQLite-only integration tests as evidence of production concurrency correctness. Domain and application ports must remain independent of whichever adapter is selected.

## Proposed implementation sequence after approval

1. Confirm the production deployment target and rough workload assumptions: concurrent jobs, poll frequency, maximum provider runtime, and expected job history retention.
2. Approve PostgreSQL or document the alternative and its transaction guarantees.
3. Choose Option A or B for durable scheduling.
4. Define migrations, unique constraints, transaction boundaries, claim/lease strategy, and recovery runbook.
5. Add adapter contract tests against the real selected database and queue.
6. Test crash windows: commit-before-ACK, publish-before-mark, worker lease expiry, duplicate poll generation, cancellation race, and dispatch retry exhaustion.
7. Add observability: oldest due/pending work age, dispatch failures, poll lag, retry counts, stuck-operation age, and manual-reconciliation queue depth.
8. Update Gates 004–006 with evidence and pass/fail status only after tests pass.

## Decisions still required from the owner

- Production hosting/deployment target.
- Whether minimizing operational components (Option A) or independent queue scaling (Option B) is more important for the first release.
- Approximate concurrency and provider polling workload.
- Retention/compliance expectations for job history and generated assets.

No database, broker, cloud, or provider vendor is selected by this document. Gate 007 remains a proposal until the owner approves a direction.
