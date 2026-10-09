# Architecture Gate 008 — Durable Request Payload and Worker Rehydration

**Status: PROPOSED — NOT APPROVED. No schema or runtime behavior changes are authorized by this document.**

## Problem

The current SubmitGenerationCommand contains job identity, capability, idempotency key, and request fingerprint, but not the original generation payload. GenerationJob and generation_jobs persist lifecycle metadata only. Meanwhile, ExecuteAsyncProviderDelivery.handle receives inputs, references, and constraints from its caller.

Therefore, a queued job cannot be reconstructed from job_id alone after a process restart that happens before a durable provider operation exists. The current crash/restart integration test proves recovery after operation + next-poll intent commit; it does not prove recovery of a not-yet-submitted request.

## Required invariants

1. A committed job has enough durable, versioned request data for a worker to resume without the original HTTP request or process memory.
2. The idempotency fingerprint is computed from the same canonical request envelope that is persisted.
3. The request payload is immutable after submission; changes require a new logical job/key.
4. Job creation, idempotency reservation, initial work intent, and request-payload persistence commit atomically.
5. Payload validation is versioned and deterministic; unsupported versions fail visibly rather than silently changing meaning.
6. Secrets, credentials, and large binary assets are not copied into arbitrary JSON fields. References must have explicit ownership/access semantics.
7. Retention/deletion behavior is defined without breaking audit history or idempotency guarantees.

## Options

### A — Add a JSONB request envelope to generation_jobs

Store schema_version, inputs, references, and constraints directly on the job row.

- **Advantages:** fewest tables and joins; job and request are naturally read together; existing job transaction makes atomicity straightforward.
- **Trade-offs:** mixes lifecycle state with potentially large request data; payload retention and access-control concerns sit on the main aggregate row; schema evolution must be handled carefully.

### B — Add an immutable generation_request_payloads table keyed by job_id

Store the versioned request envelope in a one-to-one table with a foreign key to the job, written in the same PostgreSQL transaction as the idempotency reservation, job, event, and initial work item.

- **Advantages:** separates mutable lifecycle state from immutable request data; clear access/retention boundary; still has atomic PostgreSQL commit semantics.
- **Trade-offs:** an additional repository/table and join; deletion/retention rules need explicit handling.

### C — Store payloads in object storage and keep a database reference

- **Advantages:** suitable for large request documents and binary assets; object lifecycle can be independent of relational rows.
- **Trade-offs:** object write + database commit is a dual-write problem; requires an outbox/compensation protocol, object-store selection, access controls, and cleanup of orphaned objects. More complexity than the MVP needs unless payload size demands it.

## Recommendation for review

**Recommend Option B for MVP**, with a small immutable JSONB envelope in PostgreSQL and only stable asset references—not binary media or secrets. This preserves the approved PostgreSQL-only MVP and avoids introducing object storage before there is evidence it is needed.

This recommendation is not an approved decision. Do not implement the schema until the owner confirms the option and retention/security constraints.

## Decisions needed

- Choose A, B, or C.
- Define the canonical request envelope and schema_version.
- Decide whether credentials/secrets are prohibited from inputs and how asset references are authorized.
- Set payload size limits and retention/deletion policy.
- Define whether idempotency fingerprinting is performed by the API/application layer or a shared canonicalization service, ensuring it matches the persisted envelope.

## Acceptance tests after approval

- Submission persists the payload and job atomically; rollback leaves neither.
- A fresh worker instance can rehydrate and execute a queued job without caller-supplied in-memory inputs.
- Same idempotency key + same canonical payload resolves to the same job; changed payload with the same key conflicts.
- Payload schema versions are validated and migration behavior is tested.
- Access/retention rules are tested, including referenced assets and deletion constraints.
- PostgreSQL process-restart tests cover a crash before provider submission and after provider operation + poll-intent commit.
