# AI Video Factory

AI-native content-to-video production platform.

## Current Stage

**Architecture Gate 001 — foundation**

The repository starts intentionally empty so product boundaries, domain contracts, reliability semantics, and provider abstractions can be established before implementation.

## Core principle

AI providers are replaceable infrastructure capabilities. The domain must not depend directly on Veo, Runway, ElevenLabs, OpenAI, or any other vendor.

## MVP

Idea → Script → Storyboard → Scene Generation → Voice → Composition → Captions → Quality Check → Final Video

Reliability requirements include idempotent jobs, retryable failures, selective scene regeneration, provider timeouts, cost tracking, and durable execution history.

## Engineering workflow

Understand → Map → Design → Trade-offs → Decide → TDD RED → GREEN → Refactor.

Major architectural changes require a documented design gate.

See `docs/01-architecture/ARCHITECTURE_GATE_001.md`.
