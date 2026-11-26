
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

  // Unique database object names for this test.
  const suffix = randomUUID().replaceAll('-', '');
  const functionName = `taskflow_test_fail_outbox_${suffix}`;
  const triggerName = `taskflow_test_trigger_${suffix}`;

  let functionCreated = false;
  let triggerCreated = false;

  try {
    await pool.query(
      `INSERT INTO workers (
        worker_id, worker_name, hostname, status, last_heartbeat_at
      )
      VALUES ($1, 'atomicity-test-worker', 'test-host', 'active', NOW())`,
      [workerId],
    );

    await pool.query(
      `INSERT INTO jobs (
        job_id, job_type, payload, status,
        attempts, total_attempts, max_attempts,
        locked_by, locked_at, lease_expires_at
      )
      VALUES (
        $1, 'atomicity_test', '{}'::jsonb, 'running',
        1, 1, 3, $2,
        NOW() - INTERVAL '2 minutes',
        NOW() - INTERVAL '1 minute'
      )`,
      [jobId, workerId],
    );

    await pool.query(
      `INSERT INTO job_attempts (
        job_id, worker_id, attempt_number, status
      )
      VALUES ($1, $2, 1, 'running')`,
      [jobId, workerId],
    );

    // Create a trigger that fails only for this test job.
    await pool.query(`
      CREATE FUNCTION ${functionName}()
      RETURNS trigger
      LANGUAGE plpgsql
      AS $test$
      BEGIN
        IF NEW.aggregate_id = '${jobId}'::uuid THEN
          RAISE EXCEPTION 'Simulated outbox insertion failure';
        END IF;

        RETURN NEW;
      END;
      $test$;
    `);

    functionCreated = true;

    await pool.query(`
      CREATE TRIGGER ${triggerName}
      BEFORE INSERT ON outbox_events
      FOR EACH ROW
      EXECUTE FUNCTION ${functionName}();
    `);

    triggerCreated = true;

    console.log('Test records created:', { jobId, workerId });
    console.log('Failure trigger installed.');

    // Recovery must fail when it attempts to insert the retry event.
    await assert.rejects(
      () => recoverExpiredJobs(),
      /Simulated outbox insertion failure/,
    );

    console.log('PASS: simulated outbox failure was received.');

    // The job update must have been rolled back.
    const jobResult = await pool.query(
      `SELECT status, locked_by, locked_at, lease_expires_at
       FROM jobs
       WHERE job_id = $1`,
      [jobId],
    );

    assert.equal(jobResult.rowCount, 1);
    assert.equal(jobResult.rows[0].status, 'running');
    assert.equal(jobResult.rows[0].locked_by, workerId);
    assert.ok(jobResult.rows[0].locked_at);
    assert.ok(jobResult.rows[0].lease_expires_at);

    // The attempt update must have been rolled back.
    const attemptResult = await pool.query(
      `SELECT status, finished_at, error_message
       FROM job_attempts
       WHERE job_id = $1 AND attempt_number = 1`,
      [jobId],
    );

    assert.equal(attemptResult.rowCount, 1);
    assert.equal(attemptResult.rows[0].status, 'running');
    assert.equal(attemptResult.rows[0].finished_at, null);
    assert.equal(attemptResult.rows[0].error_message, null);

    // The lifecycle event must also have been rolled back.
    const eventResult = await pool.query(
      'SELECT event_id FROM job_events WHERE job_id = $1',
      [jobId],
    );

    assert.equal(eventResult.rowCount, 0);

    // No retry outbox event should exist.
    const outboxResult = await pool.query(
      `SELECT outbox_id
       FROM outbox_events
       WHERE aggregate_id = $1`,
      [jobId],
    );

    assert.equal(outboxResult.rowCount, 0);

    console.log('PASS: job update was rolled back.');
    console.log('PASS: attempt update was rolled back.');
    console.log('PASS: lifecycle event was rolled back.');
    console.log('PASS: no outbox event was committed.');
    console.log('PASS: recovery transaction is atomic.');
  } finally {
    // Remove only the trigger and function created by this test.
    if (triggerCreated) {
      await pool.query(`DROP TRIGGER IF EXISTS ${triggerName} ON outbox_events`);
    }

    if (functionCreated) {
      await pool.query(`DROP FUNCTION IF EXISTS ${functionName}()`);
    }

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
  console.error('Lease recovery atomicity test failed:', error);
  process.exitCode = 1;
});
