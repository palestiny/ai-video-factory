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
