# Gate 006 — Concurrent Poll Scheduling Evidence

**Result: PASS for this specific concurrency invariant; Gate 006 overall remains NOT PASSED.**

Backend Tests run [#600](https://github.com/palestiny/ai-video-factory/actions/runs/37915301854) passed with **174 tests** on commit `39095c623986cf721a6debd436014ace60b682fb`.

The added real-PostgreSQL test synchronizes two independent transactions after they read the same provider-operation version, then races poll scheduling. Compare-and-swap permits only one state transition and one generation-1 poll intent; the stale writer is fenced and rolled back.

**Scope limitation:** both synchronized test workers reach the read-only provider status call before racing their writes. The test proves durable state/intent uniqueness, not single-call behavior. Production per-job worker-lease exclusivity remains a separate guard and still needs end-to-end concurrency coverage.

Gate 006 remains open for broader terminal/stale-generation/cancellation concurrency coverage, runtime entrypoint and shutdown/health lifecycle, operational visibility, and resolution of [Gate 008 — Durable Request Payload and Worker Rehydration](ARCHITECTURE_GATE_008_DURABLE_REQUEST_PAYLOAD.md). PR #1 remains Draft and unmerged.
