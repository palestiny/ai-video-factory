-- PostgreSQL baseline for AI Video Factory durable persistence + database-backed delayed work.
-- This is an initial schema contract, not a production adapter by itself.
-- Apply through the project's migration runner once one is selected.
BEGIN;

CREATE TABLE IF NOT EXISTS generation_jobs (
    job_id TEXT PRIMARY KEY,
    capability TEXT NOT NULL CHECK (length(trim(capability)) > 0),
    idempotency_key TEXT NOT NULL CHECK (length(trim(idempotency_key)) > 0),
    status TEXT NOT NULL CHECK (status IN ('QUEUED', 'RUNNING', 'SUCCEEDED', 'FAILED', 'RETRYING', 'CANCELLED')),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    failure_code TEXT,
    version BIGINT NOT NULL DEFAULT 0 CHECK (version >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_generation_jobs_status_updated
    ON generation_jobs (status, updated_at);

CREATE TABLE IF NOT EXISTS idempotency_reservations (
    scope TEXT NOT NULL CHECK (length(trim(scope)) > 0),
    idempotency_key TEXT NOT NULL CHECK (length(trim(idempotency_key)) > 0),
    request_fingerprint TEXT NOT NULL CHECK (length(trim(request_fingerprint)) > 0),
    job_id TEXT NOT NULL REFERENCES generation_jobs(job_id) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (scope, idempotency_key)
);

-- Submission reserves idempotency before inserting the job; defer this FK until commit.
ALTER TABLE idempotency_reservations
    ALTER CONSTRAINT idempotency_reservations_job_id_fkey
    DEFERRABLE INITIALLY DEFERRED;

CREATE TABLE IF NOT EXISTS generation_attempt_versions (
    attempt_id TEXT NOT NULL,
    version INTEGER NOT NULL CHECK (version >= 1),
    job_id TEXT NOT NULL REFERENCES generation_jobs(job_id) ON DELETE RESTRICT,
    attempt_number INTEGER NOT NULL CHECK (attempt_number >= 1),
    provider TEXT NOT NULL CHECK (length(trim(provider)) > 0),
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    status TEXT NOT NULL CHECK (status IN ('RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLED')),
    provider_operation_id TEXT,
    failure_code TEXT,
    PRIMARY KEY (attempt_id, version),
    CHECK (completed_at IS NULL OR completed_at >= started_at)
);

CREATE INDEX IF NOT EXISTS ix_attempt_versions_job_number
    ON generation_attempt_versions (job_id, attempt_number, version);

CREATE TABLE IF NOT EXISTS provider_operations (
    provider TEXT NOT NULL CHECK (length(trim(provider)) > 0),
    idempotency_key TEXT NOT NULL CHECK (length(trim(idempotency_key)) > 0),
    operation_id TEXT NOT NULL CHECK (length(trim(operation_id)) > 0),
    capability TEXT NOT NULL CHECK (length(trim(capability)) > 0),
    status TEXT NOT NULL CHECK (status IN ('SUBMITTED', 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLED', 'UNKNOWN')),
    submitted_at TIMESTAMPTZ,
    terminal_result JSONB,
    failure_code TEXT,
    diagnostics JSONB NOT NULL DEFAULT '{}'::jsonb,
    -- Identifies the next scheduled poll intent; increment atomically with its insertion.
    poll_generation BIGINT NOT NULL DEFAULT 0 CHECK (poll_generation >= 0),
    version BIGINT NOT NULL DEFAULT 0 CHECK (version >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (provider, idempotency_key),
    UNIQUE (provider, operation_id)
);

-- A job-execution lease is distinct from a queue-delivery claim. The worker
-- lease fences state persistence; the queue claim fences ACK/release.
CREATE TABLE IF NOT EXISTS generation_worker_leases (
    job_id TEXT PRIMARY KEY REFERENCES generation_jobs(job_id) ON DELETE CASCADE,
    worker_id TEXT NOT NULL CHECK (length(trim(worker_id)) > 0),
    lease_token UUID NOT NULL UNIQUE,
    acquired_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    CHECK (expires_at > acquired_at)
);

CREATE INDEX IF NOT EXISTS ix_generation_worker_leases_expiry
    ON generation_worker_leases (expires_at);

CREATE TABLE IF NOT EXISTS job_events (
    event_id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL REFERENCES generation_jobs(job_id) ON DELETE RESTRICT,
    event_type TEXT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    attempt_number INTEGER,
    failure_code TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS ix_job_events_job_time
    ON job_events (job_id, occurred_at, event_id);

-- A single database-backed queue: work intent is inserted in the same DB
-- transaction as the state change that requires it. Delivery is at-least-once.
CREATE TABLE IF NOT EXISTS generation_work_items (
    message_id UUID PRIMARY KEY,
    job_id TEXT NOT NULL REFERENCES generation_jobs(job_id) ON DELETE RESTRICT,
    intent_key TEXT NOT NULL UNIQUE CHECK (length(trim(intent_key)) > 0),
    intent_kind TEXT NOT NULL DEFAULT 'JOB_EXECUTION'
        CHECK (length(trim(intent_kind)) > 0),
    provider TEXT,
    operation_id TEXT,
    generation BIGINT,
    due_at TIMESTAMPTZ NOT NULL,
    state TEXT NOT NULL DEFAULT 'PENDING'
        CHECK (state IN ('PENDING', 'CLAIMED', 'ACKED', 'DEAD')),
    delivery_attempt INTEGER NOT NULL DEFAULT 1 CHECK (delivery_attempt >= 1),
    claim_token UUID,
    claimed_by TEXT,
    claimed_until TIMESTAMPTZ,
    last_error_code TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    acked_at TIMESTAMPTZ,
    CHECK (
        (intent_kind = 'PROVIDER_POLL'
            AND provider IS NOT NULL AND length(trim(provider)) > 0
            AND operation_id IS NOT NULL AND length(trim(operation_id)) > 0
            AND generation IS NOT NULL AND generation >= 1)
        OR
        (intent_kind <> 'PROVIDER_POLL'
            AND provider IS NULL AND operation_id IS NULL AND generation IS NULL)
    ),
    CHECK (
        (state = 'CLAIMED'
            AND claim_token IS NOT NULL
            AND claimed_by IS NOT NULL
            AND length(trim(claimed_by)) > 0
            AND claimed_until IS NOT NULL)
        OR
        (state <> 'CLAIMED'
            AND claim_token IS NULL
            AND claimed_by IS NULL
            AND claimed_until IS NULL)
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_work_items_provider_poll_generation
    ON generation_work_items (provider, operation_id, generation)
    WHERE intent_kind = 'PROVIDER_POLL';

CREATE INDEX IF NOT EXISTS ix_work_items_due
    ON generation_work_items (due_at, created_at)
    WHERE state = 'PENDING';

CREATE INDEX IF NOT EXISTS ix_work_items_expired_claim
    ON generation_work_items (claimed_until)
    WHERE state = 'CLAIMED';


COMMIT;
