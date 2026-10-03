# Worker / Queue Boundary — Architecture Gate 002

## Status

**Decision target:** define reliable queue/worker semantics before selecting a queue or worker technology.

## Scope

Gate 002 covers the boundary between durable generation jobs and asynchronous execution.

It defines semantics for:

- enqueueing a logical generation job
- delayed retry dispatch
- worker claim and lease ownership
- duplicate delivery
- worker crash recovery
- cancellation
- acknowledgement
- bounded retry/backoff
- provider side effects when persistence fails

It does **not** select Redis, Celery, RQ, SQS, RabbitMQ, Kafka, or a database implementation.

## Core principle

The queue is a delivery mechanism, not the source of truth.

The durable GenerationJob state and attempt history remain authoritative. Queue messages may be duplicated, delayed, lost and redelivered; application/infrastructure boundaries must make those conditions safe.

## Proposed worker lifecycle

```
QUEUED
  ↓ enqueue
DISPATCHED
  ↓ claim
RUNNING
  ├─ success → COMPLETED/SUCCEEDED
  ├─ retryable failure → FAILED → RETRYING → delayed dispatch
  ├─ terminal failure → FAILED
  └─ cancellation → CANCELLED
```

A worker claim must have an ownership/lease identity and expiry. A worker crash must not permanently strand a job in RUNNING.

## Duplicate delivery

Duplicate queue delivery is expected.

The worker must re-read durable job state before execution and use the logical job id plus attempt/execution identity to prevent two workers from independently executing the same claim at the same time.

A stale worker must not be allowed to finalize a claim after its lease has expired and ownership has moved to another worker.

## Lease semantics

A production implementation must provide:

- unique lease/worker token
- lease expiration
- atomic claim
- lease renewal for long-running generation
- ownership validation before terminal persistence
- recovery of expired RUNNING claims

Exact lease duration and renewal cadence remain deployment decisions.

## Cancellation

Cancellation is authoritative in durable job state.

A worker that observes cancellation before provider execution must not call the provider.

A worker that is already inside a provider call cannot assume the external provider supports cancellation. Provider cancellation, when available, is an adapter capability rather than a domain requirement.

A late provider result must not resurrect a cancelled logical job.

## Retry and backoff

Retry eligibility remains a domain decision.

Queue infrastructure owns:

- delay
- exponential backoff
- jitter
- dispatch
- dead-letter/terminal handling when the retry policy is exhausted

The queue must not independently increment attempt counts or redefine retryability.

## Provider side-effect boundary

Provider execution is an external side effect.

The current execution use case invokes the provider before committing the terminal persistence transaction. Therefore:

```
provider succeeds
   ↓
persistence commit fails
   ↓
external side effect may already exist
```

A retry must therefore not blindly issue a second billable/provider operation.

The provider contract must support a stable operation idempotency key or an equivalent lookup/reconciliation mechanism where the provider supports it. This is now an explicit application contract in `backend/app/application/reconciliation.py` and `docs/03-contracts/PROVIDER_RECONCILIATION_CONTRACT.md`.

Each production adapter must declare one recovery mode: `IDEMPOTENT`, `RECONCILABLE`, or `NON_RECONCILABLE`. `RECONCILABLE` adapters must reconcile before resubmission; `NON_RECONCILABLE` adapters must not silently treat an ambiguous outcome as retry-safe.

## Acknowledgement rule

A queue message may be acknowledged only after the worker has durably persisted the outcome required for that delivery.

If acknowledgement fails after durable persistence, redelivery is expected and must be harmless.

If persistence fails, the message must remain eligible for redelivery/recovery.

## Crash recovery

The system must recover at least these cases:

1. worker crashes before claim persistence
2. worker crashes after claim but before provider call
3. worker crashes during provider call
4. worker crashes after provider success but before durable completion
5. worker crashes after durable completion but before queue acknowledgement

Cases 3 and 4 are externally side-effecting ambiguity points and require provider idempotency/reconciliation semantics.

## Application contracts to establish next

Before a concrete queue adapter is introduced, establish tests/contracts for:

- `GenerationJobQueue.enqueue(job_id)`
- `GenerationJobQueue.enqueue_after(job_id, delay)`
- `GenerationJobQueue.ack(message)`
- `GenerationJobQueue.release_or_requeue(message)`
- `WorkerLeaseRepository.claim(job_id, worker_id, now, lease_duration)`
- `WorkerLeaseRepository.renew(job_id, lease_token, now, lease_duration)`
- `WorkerLeaseRepository.release(job_id, lease_token)`
- ownership-checked terminal persistence
- duplicate delivery behavior
- expired lease recovery

Names are provisional until the contracts are tested.

## Test-first acceptance criteria

The next implementation slice is accepted only when deterministic tests prove:

- one logical job cannot have two active owners
- duplicate delivery does not create a second logical job
- expired ownership can be recovered
- stale owners cannot finalize a recovered job
- cancellation is respected before provider execution
- retry delay does not change domain retry eligibility
- acknowledgement after durable completion is safe to repeat
- persistence failure leaves work recoverable
- provider-side-effect ambiguity is explicitly represented
- provider recovery safety is declared as idempotent, reconcilable, or non-reconcilable
- reconcilable recovery has an explicit lookup port and stable operation identity

## Non-decisions

This gate does not choose:

- queue technology
- broker topology
- worker runtime
- database
- lease storage technology
- provider vendor
- provider-specific cancellation behavior

## Implementation verification status

The worker orchestration boundary is now implemented as a provider/queue-neutral application service.

Verified by deterministic tests:

- queue delivery claims a worker lease before execution
- durable job state is re-read before provider execution
- cancelled and already-completed jobs are acknowledged without provider execution
- stale lease owners are rejected before provider side effects and again before terminal persistence
- terminal job state is explicitly saved inside the execution transaction
- durable success is committed before queue acknowledgement
- acknowledgement failure causes redelivery without a second provider operation when the stable provider idempotency key is honored
- expired RUNNING jobs can be recovered into RETRYING before a new attempt
- provider commit-failure redelivery reuses the same stable idempotency key
- lease loss and execution/persistence failures remain recoverable through requeue

Lease renewal is exposed as an explicit worker heartbeat contract. The worker runtime is responsible for invoking it during long-running provider calls; the application service does not hide a scheduling/threading policy inside the domain boundary.

Deterministic tests verify that renewal preserves the lease token for the current owner and rejects expired/stale owners.

The deterministic provider test double demonstrates stable operation identity across redelivery. This does **not** prove that every future provider supports idempotency. Providers without native idempotent operations still require explicit reconciliation before production adapter approval. The deterministic contract now models this distinction instead of treating every provider as retry-safe.

The first CI run for orchestration failed on exception normalization in the in-memory persistence double; the root cause was fixed. Replacement CI run #158 passed on the current head. This verifies the deterministic implementation slice only; production queue, lease-store, and provider behavior remain unverified until real adapters are introduced.

## Gate result

**PASS for boundary definition; deterministic implementation slice verified by CI. Provider reconciliation safety contract is now established; concrete provider adapters remain deferred.**

Production queue/provider adapters remain deferred until the deterministic contract suite and CI verification are green.
