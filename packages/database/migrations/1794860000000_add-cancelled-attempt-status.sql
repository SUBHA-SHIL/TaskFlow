-- Up Migration

ALTER TABLE job_attempts
DROP CONSTRAINT job_attempts_status_check;

ALTER TABLE job_attempts
ADD CONSTRAINT job_attempts_status_check
CHECK (
    status IN (
        'running',
        'completed',
        'failed',
        'abandoned',
        'cancelled'
    )
);


-- Down Migration

UPDATE job_attempts
SET status = 'abandoned'
WHERE status = 'cancelled';

ALTER TABLE job_attempts
DROP CONSTRAINT job_attempts_status_check;

ALTER TABLE job_attempts
ADD CONSTRAINT job_attempts_status_check
CHECK (
    status IN (
        'running',
        'completed',
        'failed',
        'abandoned'
    )
);
