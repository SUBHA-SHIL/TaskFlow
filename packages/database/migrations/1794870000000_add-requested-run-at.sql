-- Up Migration

ALTER TABLE jobs
ADD COLUMN requested_run_at TIMESTAMPTZ;


-- Down Migration

ALTER TABLE jobs
DROP COLUMN requested_run_at;
