import type {
  Channel,
  ChannelModel,
} from 'amqplib';

import { pool } from '@taskflow/database';

import {
  assertTaskQueue,
  consumeJobs,
  createRabbitMQChannel,
  createRabbitMQConnection,
} from '@taskflow/messaging';

import { workerEnv } from './config/env.js';
import { registerDefaultHandlers } from './features/handlers/register-handlers.js';
import { handleJobMessage } from './features/execution/job-message.handler.js';
import {
  registerWorker,
  stopWorker,
} from './features/execution/worker.repository.js';
import { startLeaseRecovery } from './features/recovery/lease-recovery.service.js';

let connection: ChannelModel | undefined;
let channel: Channel | undefined;
let workerId: string | undefined;
let leaseRecovery: ReturnType<typeof startLeaseRecovery> | undefined;

let shuttingDown = false;

async function shutdown(reason: string): Promise<void> {
  if (shuttingDown) return;

  shuttingDown = true;

  console.log(`Worker shutting down: ${reason}`);

  try {
    await leaseRecovery?.stop();
  } catch (error) {
    console.error('Failed to stop lease recovery:', error);
  }

  try {
    if (workerId) {
      await stopWorker(workerId);
    }
  } catch (error) {
    console.error('Failed to stop worker in database:', error);
  }

  try {
    if (channel) {
      await channel.close();
    }
  } catch (error) {
    console.error('Failed to close RabbitMQ channel:', error);
  }

  try {
    if (connection) {
      await connection.close();
    }
  } catch (error) {
    console.error('Failed to close RabbitMQ connection:', error);
  }

  try {
    await pool.end();
  } catch (error) {
    console.error('Failed to close PostgreSQL pool:', error);
  }

  console.log('Worker shutdown complete.');
}

async function startWorker(): Promise<void> {
  console.log('Starting TaskFlow worker...');

  // 1. Register job handlers.
  registerDefaultHandlers();

  // 2. Connect to RabbitMQ.
  connection = await createRabbitMQConnection(
    workerEnv.rabbitMQUrl,
  );

  // 3. Create channel and ensure the queue exists.
  channel = await createRabbitMQChannel(connection);

  await assertTaskQueue(channel);

  // 4. Register this worker in PostgreSQL.
  const worker = await registerWorker();

  workerId = worker.workerId;

  // 5. Start expired-job recovery.
  leaseRecovery = startLeaseRecovery();

  // 5. Start consuming messages.
  await consumeJobs(
    channel,
    (message) => handleJobMessage(message, worker.workerId),
    workerEnv.concurrency,
  );

  console.log('TaskFlow worker is running.', {
    workerId: worker.workerId,
    workerName: worker.workerName,
    concurrency: workerEnv.concurrency,
  });
}

// Graceful shutdown signals.
process.once('SIGINT', () => {
  void shutdown('SIGINT');
});

process.once('SIGTERM', () => {
  void shutdown('SIGTERM');
});

// Start the worker.
startWorker().catch(async (error: unknown) => {
  console.error('Worker startup failed:', error);

  await shutdown('startup failure');

  process.exitCode = 1;
});
