-- Up Migration

-- 1. Workers: registered worker processes
CREATE TABLE workers (
    worker_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    worker_name VARCHAR(100) NOT NULL,
    hostname TEXT,
    status VARCHAR(20) NOT NULL DEFAULT 'starting'
        CHECK (status IN ('starting', 'active', 'draining', 'stopped')),
    last_heartbeat_at TIMESTAMPTZ,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 2. Jobs: authoritative job records
CREATE TABLE jobs (
    job_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_type VARCHAR(150) NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,

    status VARCHAR(30) NOT NULL DEFAULT 'pending'
        CHECK (status IN (
            'pending',
            'queued',
            'running',
            'completed',
            'failed',
            'retry_scheduled',
            'dead',
            'cancelled',
            'scheduled'
        )),

    priority INTEGER NOT NULL DEFAULT 0,
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    max_attempts INTEGER NOT NULL DEFAULT 3 CHECK (max_attempts >= 1),

    run_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    locked_by UUID REFERENCES workers(worker_id) ON DELETE SET NULL,
    locked_at TIMESTAMPTZ,
    lease_expires_at TIMESTAMPTZ,

    completed_at TIMESTAMPTZ,
    last_error TEXT,
    result JSONB,

    idempotency_key TEXT,
    cancel_requested_at TIMESTAMPTZ,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 3. Job attempts: individual execution records
CREATE TABLE job_attempts (
    attempt_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    job_id UUID NOT NULL
        REFERENCES jobs(job_id) ON DELETE CASCADE,

    worker_id UUID
        REFERENCES workers(worker_id) ON DELETE SET NULL,

    attempt_number INTEGER NOT NULL CHECK (attempt_number >= 1),

    status VARCHAR(20) NOT NULL DEFAULT 'running'
        CHECK (status IN ('running', 'completed', 'failed', 'abandoned')),

    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,

    error_message TEXT,
    result JSONB,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_job_attempt_number
        UNIQUE (job_id, attempt_number)
);

-- 4. Job events: lifecycle and state transition history
CREATE TABLE job_events (
    event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    job_id UUID NOT NULL
        REFERENCES jobs(job_id) ON DELETE CASCADE,

    worker_id UUID
        REFERENCES workers(worker_id) ON DELETE SET NULL,

    event_type VARCHAR(100) NOT NULL,
    previous_status VARCHAR(30),
    new_status VARCHAR(30),

    actor_type VARCHAR(20) NOT NULL DEFAULT 'system'
        CHECK (actor_type IN ('system', 'api', 'worker')),

    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 5. Outbox events: reliable event publication
CREATE TABLE outbox_events (
    outbox_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    aggregate_type VARCHAR(100) NOT NULL,
    aggregate_id UUID NOT NULL,
    event_type VARCHAR(150) NOT NULL,

    payload JSONB NOT NULL,

    status VARCHAR(20) NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'processing', 'published')),

    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),

    available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    locked_at TIMESTAMPTZ,
    locked_by TEXT,

    published_at TIMESTAMPTZ,
    last_error TEXT,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Indexes for job dispatch and recovery
CREATE INDEX idx_jobs_status_run_at_priority
    ON jobs (status, run_at, priority DESC, created_at);

CREATE INDEX idx_jobs_running_lease
    ON jobs (lease_expires_at)
    WHERE status = 'running';

CREATE UNIQUE INDEX idx_jobs_idempotency_key
    ON jobs (idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE INDEX idx_jobs_created_at
    ON jobs (created_at DESC);

-- Indexes for execution history
CREATE INDEX idx_job_attempts_job_created
    ON job_attempts (job_id, created_at DESC);

CREATE INDEX idx_job_events_job_created
    ON job_events (job_id, created_at DESC);

-- Indexes for worker monitoring
CREATE INDEX idx_workers_status_heartbeat
    ON workers (status, last_heartbeat_at);

-- Indexes for outbox publishing and recovery
CREATE INDEX idx_outbox_pending
    ON outbox_events (available_at, created_at)
    WHERE status = 'pending';

CREATE INDEX idx_outbox_processing
    ON outbox_events (locked_at)
    WHERE status = 'processing';


-- Down Migration

DROP TABLE IF EXISTS outbox_events;
DROP TABLE IF EXISTS job_events;
DROP TABLE IF EXISTS job_attempts;
DROP TABLE IF EXISTS jobs;
DROP TABLE IF EXISTS workers;
