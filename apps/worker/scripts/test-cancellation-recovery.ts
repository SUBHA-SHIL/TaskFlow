
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
    // Create an isolated worker.
    await pool.query(
      `INSERT INTO workers (
        worker_id,
        worker_name,
        hostname,
        status,
        last_heartbeat_at
      )
      VALUES ($1, $2, $3, 'active', NOW())`,
      [workerId, 'cancellation-recovery-test', 'test-host'],
    );

    // Create a running job with an expired lease and cancellation request.
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
        lease_expires_at,
        cancel_requested_at
      )
      VALUES (
        $1,
        'cancellation_recovery_test',
        '{}'::jsonb,
        'running',
        1,
        1,
        3,
        $2,
        NOW() - INTERVAL '2 minutes',
        NOW() - INTERVAL '1 minute',
        NOW() - INTERVAL '30 seconds'
      )`,
      [jobId, workerId],
    );

    // Create its currently running attempt.
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

    // Execute the actual recovery implementation.
    const recoveredCount = await recoverExpiredJobs();

    console.log('Recovered jobs:', recoveredCount);

    // Verify the job state.
    const jobResult = await pool.query(
      `SELECT
        status,
        locked_by,
        locked_at,
        lease_expires_at,
        cancel_requested_at,
        completed_at,
        last_error
      FROM jobs
      WHERE job_id = $1`,
      [jobId],
    );

    assert.equal(jobResult.rowCount, 1);

    const job = jobResult.rows[0];

    assert.equal(job.status, 'cancelled');
    assert.equal(job.locked_by, null);
    assert.equal(job.locked_at, null);
    assert.equal(job.lease_expires_at, null);
    assert.ok(job.completed_at);
    assert.equal(job.last_error, null);

    // Verify the attempt was cancelled.
    const attemptResult = await pool.query(
      `SELECT status, error_message, finished_at
       FROM job_attempts
       WHERE job_id = $1 AND attempt_number = 1`,
      [jobId],
    );

    assert.equal(attemptResult.rowCount, 1);
    assert.equal(attemptResult.rows[0].status, 'cancelled');
    assert.ok(attemptResult.rows[0].error_message);
    assert.ok(attemptResult.rows[0].finished_at);

    // Verify the cancellation event.
    const eventResult = await pool.query(
      `SELECT event_type, new_status
       FROM job_events
       WHERE job_id = $1`,
      [jobId],
    );

    assert.ok(
      eventResult.rows.some(
        (event) =>
          event.event_type === 'job.cancelled' &&
          event.new_status === 'cancelled',
      ),
    );

    // Verify no retry outbox event was created.
    const outboxResult = await pool.query(
      `SELECT outbox_id
       FROM outbox_events
       WHERE aggregate_id = $1
         AND event_type = 'job.retry_scheduled'`,
      [jobId],
    );

    assert.equal(outboxResult.rowCount, 0);

    assert.ok(recoveredCount >= 1);

    console.log('PASS: expired cancellation request recovered correctly.');
    console.log('PASS: job status is cancelled.');
    console.log('PASS: attempt status is cancelled.');
    console.log('PASS: worker lease and ownership are cleared.');
    console.log('PASS: cancellation event was recorded.');
    console.log('PASS: no retry outbox event was created.');
  } finally {
    // Remove only records created by this test.
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
  console.error('Cancellation recovery test failed:', error);
  process.exitCode = 1;
});
