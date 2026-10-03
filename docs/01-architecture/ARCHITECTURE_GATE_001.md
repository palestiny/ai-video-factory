# Architecture Gate 001 — Production Core

## Status

**Decision target:** establish the foundation for implementation.

## Architectural decision

Use a provider-neutral modular architecture with explicit domain, application, and infrastructure boundaries.

```
API
 ↓
Application Use Cases
 ↓
Domain Model / Ports
 ↓
Infrastructure Adapters
 ├─ Video provider
 ├─ Image provider
 ├─ Voice provider
 ├─ LLM provider
 ├─ Storage
 └─ Rendering
```

The domain never imports vendor SDKs.

## Core domains

1. Content
2. Generation
3. Assets
4. Rendering
5. Quality
6. Operations

## Lifecycle

```
DRAFT
  ↓
PLANNING
  ↓
STORYBOARD_READY
  ↓
GENERATING
  ↓
ASSETS_READY
  ↓
RENDERING
  ↓
QUALITY_CHECK
  ↓
COMPLETED
```

Failure is explicit:

```
RUNNING → FAILED → RETRYING → RUNNING
```

Cancellation is terminal for the requested operation.

## Generation job

A GenerationJob represents one externally executable generation intent.

Required concepts:
- stable job identity
- project/video/scene association
- capability type
- provider/model selection
- normalized input
- idempotency key
- status
- attempt history
- output artifact reference
- failure information
- cost
- timestamps

## Provider abstraction

The application depends on ports such as:

- VideoGenerationPort
- ImageGenerationPort
- VoiceGenerationPort
- TextGenerationPort

Adapters translate provider-specific requests/responses into domain-neutral results.

## Reliability principles

- At-least-once execution must not create duplicate logical jobs.
- Every external attempt is recorded.
- Retries are bounded by policy.
- Provider timeout is distinct from provider rejection.
- A completed generation is reusable.
- Scene regeneration does not invalidate unrelated completed scenes.
- Every billable provider call has a cost record when cost data is available.

## Important non-decisions

We do **not** lock the system to:
- one video provider
- one LLM
- one TTS vendor
- one queue implementation
- one database vendor
- one rendering service

## Gate result

**PASS for foundation design.**

Implementation may begin with domain contracts and tests. Concrete provider integration is deferred until those contracts are stable.

## Implementation Progress

The foundation is now being implemented incrementally behind the approved boundaries:

- GenerationJob lifecycle and retry policy
- immutable GenerationAttempt history
- atomic idempotency repository contract with request fingerprint
- normalized Failure model
- immutable JobEvent model
- provider-neutral generation ports and deterministic fake contract
- shared durable persistence contracts for jobs, attempts, events, and transactional boundaries

Still intentionally deferred:

- concrete provider SDK adapters
- production persistence implementation
- queue/worker implementation
- rendering implementation
- external API integration
- frontend

### Transaction boundary note

Idempotency reservation and creation/persistence of the corresponding logical job must share an atomic application/infrastructure transaction in production. A reservation must never survive a failed job creation as an orphaned claim.

### Execution Use Case Progress

Generation execution is now represented as an application use case rather than a provider-specific service. The use case resolves a provider by capability, creates an immutable attempt, invokes the normalized provider port, normalizes failures, records terminal attempt state, and emits lifecycle events. Retry scheduling remains outside this boundary so queue/backoff infrastructure is not coupled to provider execution.

### Execution transaction safety

The execution boundary treats commit failure as a transaction failure: commit exceptions trigger rollback before the exception is propagated. This keeps provider execution from reporting success/failure as durably completed when the persistence transaction did not commit.

### Retry scheduling progress

Retry eligibility is now separated from retry dispatch. The application use case transitions a failed logical job to RETRYING only when the domain policy allows another attempt and records RETRY_SCHEDULED atomically with the state change. Queue delay, exponential backoff/jitter, and worker dispatch remain infrastructure concerns.

### Persistence Contract Progress

Persistence is now an explicit application boundary rather than being redefined independently by each use case.

The shared contract covers:
- logical GenerationJob retrieval and creation
- immutable GenerationAttempt recording
- append-only JobEvent storage
- atomic commit/rollback boundaries
- submission transactions that include idempotency
- execution transactions that include attempt persistence

No database, ORM, queue, or vendor-specific storage technology is selected by this gate. Concrete persistence remains an infrastructure decision after the contract is proven by tests.

### Immutable attempt persistence decision

Generation attempts remain append-only at the persistence boundary. The application records the initial RUNNING attempt with `add()`, then records its terminal immutable record with `complete()`. The contract intentionally does not expose a `replace()` operation, because replacing a committed attempt would conflict with the reliability requirement that attempt history remain immutable and auditable.

### Deterministic persistence verification

The persistence contract is now exercised by a deterministic in-memory implementation and integration tests covering:

- atomic submission persistence for job, lifecycle event, and idempotency reservation
- rollback of submission state when commit fails
- idempotency conflict detection
- append-only attempt history
- rollback restoration of attempts and events

The in-memory implementation is a test double, not a production transaction engine. It intentionally does not provide database-level concurrency isolation, locking, durability, or ORM dirty-tracking semantics. Those properties remain requirements for the future infrastructure implementation.

### Gate 002 dependency

The next architectural boundary is Worker / Queue reliability. Queue technology remains intentionally unselected until claim/lease ownership, duplicate delivery, crash recovery, cancellation, acknowledgement, and provider-side-effect ambiguity are represented by deterministic contracts and tests.

