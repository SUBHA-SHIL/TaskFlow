
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';

import type {
  Channel,
  ConfirmChannel,
  ConsumeMessage,
} from 'amqplib';

import {
  pool,
  closeDatabaseConnection,
} from '@taskflow/database';

import { consumeJobs } from '@taskflow/messaging';

import { executeJob } from '../src/features/execution/job-execution.service.js';
import {
  registerHandler,
} from '../src/features/handlers/handler.registry.js';

async function main(): Promise<void> {
  const jobId = randomUUID();
  const workerId = randomUUID();

  let signalHandlerStarted!: () => void;
  let releaseHandler!: () => void;
  let resolveDuplicateAck!: () => void;

  const handlerStarted = new Promise<void>((resolve) => {
    signalHandlerStarted = resolve;
  });

  const handlerGate = new Promise<void>((resolve) => {
    releaseHandler = resolve;
  });

  const duplicateAcked = new Promise<void>((resolve) => {
    resolveDuplicateAck = resolve;
  });

  let handlerExecutionCount = 0;
  let acknowledgementCount = 0;
  let rejectionCount = 0;

  let messageCallback:
    | ((message: ConsumeMessage | null) => void)
    | undefined;

  let consumer:
    | Awaited<ReturnType<typeof consumeJobs>>
    | undefined;

  try {
    // Register the worker.
    await pool.query(
      `INSERT INTO workers (
        worker_id, worker_name, hostname, status, last_heartbeat_at
      )
      VALUES ($1, 'duplicate-ack-test-worker', 'test-host',
              'active', NOW())`,
      [workerId],
    );

    // Create one eligible job.
    await pool.query(
      `INSERT INTO jobs (
        job_id, job_type, payload, status,
        attempts, total_attempts, max_attempts, run_at
      )
      VALUES ($1, 'duplicate_ack_test', '{}'::jsonb,
              'pending', 0, 0, 3, NOW())`,
      [jobId],
    );

    // Register a handler that waits until released.
    registerHandler('duplicate_ack_test', async () => {
      handlerExecutionCount++;
      signalHandlerStarted();

      await handlerGate;

      return { success: true };
    });

    // Simulate the RabbitMQ channel.
    const fakeChannel = {
      prefetch: async () => undefined,

      consume: async (
        _queue: string,
        callback: (message: ConsumeMessage | null) => void,
      ) => {
        messageCallback = callback;

        return {
          consumerTag: 'duplicate-ack-test-consumer',
        };
      },

      ack: (_message: ConsumeMessage) => {
        acknowledgementCount++;

        if (acknowledgementCount === 1) {
          resolveDuplicateAck();
        }
      },

      nack: () => {
        rejectionCount++;
      },

      cancel: async () => undefined,
    } as unknown as Channel;

    const fakeRetryPublisher =
      {} as unknown as ConfirmChannel;

    consumer = await consumeJobs(
      fakeChannel,
      fakeRetryPublisher,
      async (message) => {
        await executeJob(message.jobId, workerId);
      },
      2,
      (error) => {
        throw error;
      },
    );

    assert.ok(messageCallback);

    const rawMessage = {
      content: Buffer.from(
        JSON.stringify({
          jobId,
          jobType: 'duplicate_ack_test',
        }),
      ),
      properties: {
        headers: {},
      },
    } as ConsumeMessage;

    console.log('Delivering first message...');

    messageCallback(rawMessage);

    await handlerStarted;

    console.log('First handler is running.');

    console.log('Delivering duplicate message...');

    messageCallback(rawMessage);

    // The duplicate should be acknowledged while the
    // first execution is still running.
    await duplicateAcked;

    assert.equal(acknowledgementCount, 1);
    assert.equal(rejectionCount, 0);
    assert.equal(handlerExecutionCount, 1);

    console.log(
      'PASS: duplicate delivery was acknowledged.',
    );

    console.log(
      'PASS: duplicate delivery did not execute the handler.',
    );

    // Allow the original execution to finish.
    releaseHandler();

    await consumer.stop();

    assert.equal(acknowledgementCount, 2);
    assert.equal(rejectionCount, 0);
    assert.equal(handlerExecutionCount, 1);

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

    console.log('PASS: original delivery was acknowledged.');
    console.log('PASS: both deliveries were acknowledged.');
    console.log('PASS: no delivery was rejected.');
    console.log('PASS: exactly one handler execution occurred.');
    console.log('PASS: exactly one database attempt was recorded.');
    console.log('PASS: duplicate message acknowledgement test passed.');
  } finally {
    // Never leave the handler blocked.
    releaseHandler?.();

    if (consumer) {
      await consumer.stop();
    }

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
      'DELETE FROM workers WHERE worker_id = $1',
      [workerId],
    );

    await closeDatabaseConnection();

    console.log('Test cleanup completed.');
  }
}

main().catch((error: unknown) => {
  console.error(
    'Duplicate message acknowledgement test failed:',
    error,
  );

  process.exitCode = 1;
});
