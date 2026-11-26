
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';

import {
  pool,
  closeDatabaseConnection,
} from '@taskflow/database';

import { executeJob } from '../src/features/execution/job-execution.service.js';
import {
  registerHandler,
} from '../src/features/handlers/handler.registry.js';

async function main(): Promise<void> {
  const jobId = randomUUID();
  const workerA = randomUUID();
  const workerB = randomUUID();

  let signalHandlerStarted!: () => void;
  let releaseHandler!: () => void;

  const handlerStarted = new Promise<void>((resolve) => {
    signalHandlerStarted = resolve;
  });

  const handlerGate = new Promise<void>((resolve) => {
    releaseHandler = resolve;
  });

  let handlerExecutionCount = 0;

  try {
    // Register two active workers.
    await pool.query(
      `INSERT INTO workers (
        worker_id, worker_name, hostname, status, last_heartbeat_at
      )
      VALUES
        ($1, 'duplicate-test-worker-a', 'test-host', 'active', NOW()),
        ($2, 'duplicate-test-worker-b', 'test-host', 'active', NOW())`,
      [workerA, workerB],
    );

    // Create one eligible job.
    await pool.query(
      `INSERT INTO jobs (
        job_id, job_type, payload, status, attempts,
        total_attempts, max_attempts, run_at
      )
      VALUES ($1, 'duplicate_execution_test', '{}'::jsonb,
              'pending', 0, 0, 3, NOW())`,
      [jobId],
    );

    // Register a handler that stays running until released.
    registerHandler('duplicate_execution_test', async () => {
      handlerExecutionCount++;
      signalHandlerStarted();

      await handlerGate;

      return { success: true };
    });

    console.log('Starting first execution...');

    const firstExecution = executeJob(jobId, workerA);

    // Wait until the first worker has claimed the job
    // and entered the handler.
    await handlerStarted;

    console.log('First handler is running.');
    console.log('Starting duplicate execution...');

    // A different worker attempts to execute the same job.
    const secondResult = await executeJob(jobId, workerB);

    assert.deepEqual(secondResult, {
      status: 'not_claimed',
    });

    assert.equal(handlerExecutionCount, 1);

    console.log('PASS: second worker could not claim the running job.');
    console.log('PASS: duplicate handler execution was prevented.');

    // Allow the first execution to finish.
    releaseHandler();

    const firstResult = await firstExecution;

    assert.deepEqual(firstResult, {
      status: 'completed',
    });

    // Verify the final database state.
    const jobResult = await pool.query(
      `SELECT status, attempts, total_attempts, locked_by
       FROM jobs
       WHERE job_id = $1`,
      [jobId],
    );

    assert.equal(jobResult.rowCount, 1);
    assert.equal(jobResult.rows[0].status, 'completed');
    assert.equal(jobResult.rows[0].attempts, 1);
    assert.equal(jobResult.rows[0].total_attempts, 1);
    assert.equal(jobResult.rows[0].locked_by, null);

    const attemptResult = await pool.query(
      `SELECT COUNT(*)::integer AS count
       FROM job_attempts
       WHERE job_id = $1`,
      [jobId],
    );

    assert.equal(attemptResult.rows[0].count, 1);

    assert.equal(handlerExecutionCount, 1);

    console.log('PASS: first execution completed successfully.');
    console.log('PASS: exactly one execution attempt was recorded.');
    console.log('PASS: final job status is completed.');
    console.log('PASS: duplicate execution test passed.');
  } finally {
    // Ensure a waiting handler cannot remain blocked.
    releaseHandler?.();

    await pool.query(
      'DELETE FROM outbox_events WHERE aggregate_id = $1',
      [jobId],
    );

    await pool.query(
      'DELETE FROM job_events WHERE job_id = $1',
      [jobId],
    );

    await pool.query(
      'DELETE FROM job_attempts WHERE job_id = $1',
      [jobId],
    );

    await pool.query(
      'DELETE FROM jobs WHERE job_id = $1',
      [jobId],
    );

    await pool.query(
      'DELETE FROM workers WHERE worker_id = ANY($1::uuid[])',
      [[workerA, workerB]],
    );

    await closeDatabaseConnection();

    console.log('Test cleanup completed.');
  }
}

main().catch((error: unknown) => {
  console.error('Duplicate execution test failed:', error);
  process.exitCode = 1;
});
