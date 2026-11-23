-- Up Migration

ALTER TABLE workers
DROP CONSTRAINT workers_status_check;

ALTER TABLE workers
ADD CONSTRAINT workers_status_check
CHECK (
    status IN (
        'starting',
        'active',
        'draining',
        'stopped',
        'lost'
    )
);


-- Down Migration

UPDATE workers
SET status = 'stopped'
WHERE status = 'lost';

ALTER TABLE workers
DROP CONSTRAINT workers_status_check;

ALTER TABLE workers
ADD CONSTRAINT workers_status_check
CHECK (
    status IN (
        'starting',
        'active',
        'draining',
        'stopped'
    )
);