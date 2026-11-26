
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
      [workerId, 'exhausted-recovery-test', 'test-host'],
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
        'exhausted_recovery_test',
        '{}'::jsonb,
        'running',
        3,
        3,
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
      VALUES ($1, $2, 3, 'running')`,
      [jobId, workerId],
    );

    console.log('Test records created:', { jobId, workerId });

    const recoveredCount = await recoverExpiredJobs();

    console.log('Recovered jobs:', recoveredCount);

    // Verify that the job has permanently failed.
    const jobResult = await pool.query(
      `SELECT
        status,
        attempts,
        locked_by,
        locked_at,
        lease_expires_at,
        completed_at,
        last_error
      FROM jobs
      WHERE job_id = $1`,
      [jobId],
    );

    assert.equal(jobResult.rowCount, 1);

    const job = jobResult.rows[0];

    assert.equal(job.status, 'dead');
    assert.equal(job.attempts, 3);
    assert.equal(job.locked_by, null);
    assert.equal(job.locked_at, null);
    assert.equal(job.lease_expires_at, null);
    assert.ok(job.completed_at);
    assert.ok(job.last_error);

    // Verify that the final attempt was abandoned.
    const attemptResult = await pool.query(
      `SELECT status, error_message, finished_at
       FROM job_attempts
       WHERE job_id = $1 AND attempt_number = 3`,
      [jobId],
    );

    assert.equal(attemptResult.rowCount, 1);
    assert.equal(attemptResult.rows[0].status, 'abandoned');
    assert.ok(attemptResult.rows[0].error_message);
    assert.ok(attemptResult.rows[0].finished_at);

    // Verify that no retry event was created.
    const outboxResult = await pool.query(
      `SELECT outbox_id
       FROM outbox_events
       WHERE aggregate_id = $1
         AND event_type = 'job.retry_scheduled'`,
      [jobId],
    );

    assert.equal(outboxResult.rowCount, 0);

    // Verify the lease-expired lifecycle event.
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
          event.new_status === 'dead',
      ),
    );

    assert.ok(recoveredCount >= 1);

    console.log('PASS: exhausted job entered dead status.');
    console.log('PASS: final attempt was marked abandoned.');
    console.log('PASS: worker lease and ownership were cleared.');
    console.log('PASS: completed_at was recorded.');
    console.log('PASS: no retry outbox event was created.');
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
  console.error('Exhausted lease recovery test failed:', error);
  process.exitCode = 1;
});
