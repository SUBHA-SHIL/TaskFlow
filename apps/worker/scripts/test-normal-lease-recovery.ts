
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';

import {
  pool,
  closeDatabaseConnection,
} from '@taskflow/database';

import { recoverExpiredJobs } from '../src/features/recovery/lease-recovery.repository.js';

async function main(): Promise<void> {
  const workerId = randomUUID();
  const jobId = randomUUID();

  try {
    await pool.query(
      `INSERT INTO workers (
        worker_id,
        worker_name,
        hostname,
        status,
        last_heartbeat_at
      )
      VALUES ($1, $2, $3, 'active', NOW())`,
      [workerId, 'normal-recovery-test', 'test-host'],
    );

    await pool.query(
      `INSERT INTO jobs (
        job_id,
        job_type,
        payload,
        status,
        attempts,
        total_attempts,
        max_attempts,
        locked_by,
        locked_at,
        lease_expires_at
      )
      VALUES (
        $1,
        'normal_recovery_test',
        '{}'::jsonb,
        'running',
        1,
        1,
        3,
        $2,
        NOW() - INTERVAL '2 minutes',
        NOW() - INTERVAL '1 minute'
      )`,
      [jobId, workerId],
    );

    await pool.query(
      `INSERT INTO job_attempts (
        job_id,
        worker_id,
        attempt_number,
        status
      )
      VALUES ($1, $2, 1, 'running')`,
      [jobId, workerId],
    );

    console.log('Test records created:', { jobId, workerId });

    const recoveredCount = await recoverExpiredJobs();

    console.log('Recovered jobs:', recoveredCount);

    // Verify that the job is scheduled for retry.
    const jobResult = await pool.query(
      `SELECT
        status,
        attempts,
        locked_by,
        locked_at,
        lease_expires_at,
        run_at,
        last_error,
        completed_at
      FROM jobs
      WHERE job_id = $1`,
      [jobId],
    );

    assert.equal(jobResult.rowCount, 1);

    const job = jobResult.rows[0];

    assert.equal(job.status, 'retry_scheduled');
    assert.equal(job.attempts, 1);
    assert.equal(job.locked_by, null);
    assert.equal(job.locked_at, null);
    assert.equal(job.lease_expires_at, null);
    assert.ok(job.run_at > new Date());
    assert.ok(job.last_error);
    assert.equal(job.completed_at, null);

    // Verify that the expired attempt was abandoned.
    const attemptResult = await pool.query(
      `SELECT status, error_message, finished_at
       FROM job_attempts
       WHERE job_id = $1 AND attempt_number = 1`,
      [jobId],
    );

    assert.equal(attemptResult.rowCount, 1);
    assert.equal(attemptResult.rows[0].status, 'abandoned');
    assert.ok(attemptResult.rows[0].error_message);
    assert.ok(attemptResult.rows[0].finished_at);

    // Verify that a retry outbox event was created.
    const outboxResult = await pool.query(
      `SELECT event_type, status, available_at
       FROM outbox_events
       WHERE aggregate_id = $1
         AND event_type = 'job.retry_scheduled'`,
      [jobId],
    );

    assert.equal(outboxResult.rowCount, 1);
    assert.equal(outboxResult.rows[0].status, 'pending');

    // Verify that the lease-expired lifecycle event was recorded.
    const eventResult = await pool.query(
      `SELECT event_type, new_status
       FROM job_events
       WHERE job_id = $1`,
      [jobId],
    );

    assert.ok(
      eventResult.rows.some(
        (event) =>
          event.event_type === 'job.lease_expired' &&
          event.new_status === 'retry_scheduled',
      ),
    );

    assert.ok(recoveredCount >= 1);

    console.log('PASS: expired job entered retry_scheduled.');
    console.log('PASS: expired attempt was marked abandoned.');
    console.log('PASS: worker lease and ownership were cleared.');
    console.log('PASS: retry backoff was scheduled.');
    console.log('PASS: retry outbox event was created.');
    console.log('PASS: lease-expired event was recorded.');
  } finally {
    await pool.query(
      'DELETE FROM outbox_events WHERE aggregate_id = $1',
      [jobId],
    );

    await pool.query(
      'DELETE FROM jobs WHERE job_id = $1',
      [jobId],
    );

    await pool.query(
      'DELETE FROM workers WHERE worker_id = $1',
      [workerId],
    );

    await closeDatabaseConnection();
    console.log('Test cleanup completed.');
  }
}

main().catch((error: unknown) => {
  console.error('Normal lease recovery test failed:', error);
  process.exitCode = 1;
});
