# Product Vision

## Problem

Producing repeatable AI videos is not one generation request. It is a multi-stage production workflow involving planning, visual continuity, generation, voice, composition, validation, and recovery.

## Product

AI Video Factory orchestrates that workflow as a durable production system.

## Primary value

- Turn a content idea into a production-ready video.
- Regenerate failed or weak scenes without rebuilding the whole video.
- Keep provider integrations replaceable.
- Preserve execution history, artifacts, costs, and decisions.
- Support deterministic testing without spending on external AI APIs.

## MVP boundaries

### In scope
- Project/video creation
- Script generation contract
- Storyboard and scene model
- Image/video/voice provider ports
- Generation jobs and attempts
- Retry and idempotency semantics
- Asset registration
- Render composition contract
- Captions
- Basic quality validation
- Cost records
- Final artifact lifecycle

### Out of scope for MVP
- Social publishing
- Audience analytics
- Trend discovery
- Autonomous content discovery
- Multi-tenant billing
- Provider marketplace
- Fully autonomous optimization

These are future capabilities, not prerequisites for a sound production core.
