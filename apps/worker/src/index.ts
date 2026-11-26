import type {
  Channel,
  ChannelModel,
} from 'amqplib';

import { pool } from '@taskflow/database';

import {
  assertTaskQueue,
  consumeJobs,
  createRabbitMQChannel,
  createRabbitMQConfirmChannel,
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
import { startWorkerHeartbeat } from './features/execution/worker-heartbeat.service.js';
import { withTimeout } from './utils/with-timeout.js';

let connection: ChannelModel | undefined;
let channel: Channel | undefined;
let retryPublisherChannel:
  | Awaited<ReturnType<typeof createRabbitMQConfirmChannel>>
  | undefined;
let workerId: string | undefined;

const SHUTDOWN_TIMEOUT_MS = 30_000;

let leaseRecovery:
  | ReturnType<typeof startLeaseRecovery>
  | undefined;

let workerHeartbeat:
  | ReturnType<typeof startWorkerHeartbeat>
  | undefined;

const jobConsumerRef: {
  current: Awaited<ReturnType<typeof consumeJobs>> | undefined;
} = {
  current: undefined,
};

let shuttingDown = false;


async function forceShutdown(reason: string): Promise<never> {
  console.error(`Forcing worker shutdown: ${reason}`);

  try {
    if (connection) {
      await withTimeout(
        connection.close(),
        5_000,
        'RabbitMQ connection close timed out',
      );
    }
  } catch (error) {
    console.error(
      'Failed to close RabbitMQ connection during forced shutdown:',
      error,
    );
  }

  // Do not mark the worker stopped or close PostgreSQL gracefully.
  // The process termination stops its heartbeat and lets stale-worker
  // recovery identify it as lost.
  process.exit(1);
}


async function shutdown(reason: string): Promise<void> {
  if (shuttingDown) return;

  shuttingDown = true;

  console.log(`Worker shutting down: ${reason}`);

  // 1. Stop accepting new messages.
  try {
    await jobConsumerRef.current?.stopReceiving();
  } catch (error) {
    console.error('Failed to stop RabbitMQ consumer:', error);

    await forceShutdown('Unable to stop receiving messages');
  }

  // 2. Wait for active jobs, with a 30-second deadline.
  try {
    await withTimeout(
      jobConsumerRef.current?.waitForInFlight() ?? Promise.resolve(),
      SHUTDOWN_TIMEOUT_MS,
      'Graceful shutdown timeout exceeded',
    );
  } catch (error) {
    console.error(
      'Worker could not finish all jobs before shutdown deadline:',
      error,
    );

    await forceShutdown('Graceful shutdown deadline exceeded');
  }

  // 3. Stop lease recovery.
  try {
    await leaseRecovery?.stop();
  } catch (error) {
    console.error('Failed to stop lease recovery:', error);
  }

  // 4. Stop heartbeat.
  try {
    await workerHeartbeat?.stop();
  } catch (error) {
    console.error('Failed to stop worker heartbeat:', error);
  }

  // 5. Mark worker as stopped.
  try {
    if (workerId) {
      await stopWorker(workerId);
    }
  } catch (error) {
    console.error('Failed to stop worker in database:', error);
  }

  // 6. Close RabbitMQ channels independently.
  try {
    if (channel) {
      await channel.close();
    }
  } catch (error) {
    console.error('Failed to close RabbitMQ consumer channel:', error);
  }

  try {
    if (retryPublisherChannel) {
      await retryPublisherChannel.close();
    }
  } catch (error) {
    console.error('Failed to close RabbitMQ retry publisher channel:', error);
  }

  // 7. Close RabbitMQ connection.
  try {
    if (connection) {
      await connection.close();
    }
  } catch (error) {
    console.error('Failed to close RabbitMQ connection:', error);
  }

  // 8. Close PostgreSQL connection pool.
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

  // 3. Create consumer and retry publisher channels.
  channel = await createRabbitMQChannel(connection);

  retryPublisherChannel =
    await createRabbitMQConfirmChannel(connection);

  await assertTaskQueue(channel);
  await assertTaskQueue(retryPublisherChannel);

  // 4. Register this worker in PostgreSQL.
  const worker = await registerWorker();

  workerId = worker.workerId;
  console.log('WORKER_PROCESS_IDENTITY', {
    workerId: worker.workerId,
    processId: process.pid,
    parentProcessId: process.ppid,
  });

  // 5. Start worker heartbeat.
  workerHeartbeat = startWorkerHeartbeat(
    worker.workerId,
    (error) => {
      console.error(
        'Worker is shutting down because heartbeat failed.',
        error,
      );

      void shutdown('heartbeat failure');
    },
  );

  // 6. Start expired-job recovery.
  leaseRecovery = startLeaseRecovery();

  // 7. Start consuming messages.
  jobConsumerRef.current = await consumeJobs(
    channel,
    retryPublisherChannel,
    (message) => handleJobMessage(message, worker.workerId),
    workerEnv.concurrency,
    (error) => {
      console.error(
        'Worker is shutting down because retry publishing failed.',
        error,
      );

      void shutdown('retry publisher failure');
    },
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
