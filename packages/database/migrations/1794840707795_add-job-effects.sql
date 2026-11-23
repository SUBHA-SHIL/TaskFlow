-- Up Migration

CREATE TABLE job_effects (
    effect_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    job_id UUID NOT NULL
        REFERENCES jobs(job_id) ON DELETE CASCADE,

    effect_key VARCHAR(255) NOT NULL,

    status VARCHAR(20) NOT NULL DEFAULT 'completed'
        CHECK (status IN ('completed')),

    result JSONB,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_job_effect_key
        UNIQUE (job_id, effect_key)
);

CREATE INDEX idx_job_effects_job_created
    ON job_effects (job_id, created_at DESC);


-- Down Migration

DROP TABLE IF EXISTS job_effects;