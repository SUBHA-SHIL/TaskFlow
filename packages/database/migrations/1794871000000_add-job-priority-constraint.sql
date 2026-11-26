-- Up Migration

ALTER TABLE jobs
ADD CONSTRAINT jobs_priority_range_check
CHECK (priority BETWEEN 0 AND 10);


-- Down Migration

ALTER TABLE jobs
DROP CONSTRAINT jobs_priority_range_check;