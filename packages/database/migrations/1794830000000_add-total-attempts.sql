-- Up Migration

-- Track the lifetime number of execution attempts independently
-- from the current retry-cycle counter.
ALTER TABLE jobs
ADD COLUMN total_attempts INTEGER NOT NULL DEFAULT 0
CHECK (total_attempts >= 0);

-- Preserve the highest historical attempt number for every job.
UPDATE jobs AS j
SET total_attempts = GREATEST(
    j.attempts,
    COALESCE(
        (
            SELECT MAX(ja.attempt_number)
            FROM job_attempts AS ja
            WHERE ja.job_id = j.job_id
        ),
        0
    )
);

-- Down Migration

ALTER TABLE jobs
DROP COLUMN total_attempts;