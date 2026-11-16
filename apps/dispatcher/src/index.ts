import { randomUUID } from 'node:crypto';

import {
  closeDatabaseConnection,
} from '@taskflow/database';

import {
  assertTaskQueue,
  createRabbitMQConnection,
  createRabbitMQConfirmChannel,
} from '@taskflow/messaging';

import { env } from './config/env.js';
import { publishPendingOutboxEvents } from './features/outbox-publisher/outbox.service.js';

const dispatcherId = randomUUID();

let isShuttingDown = false;

function sleep(milliseconds: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, milliseconds);
  });
}

async function main(): Promise<void> {
  const rabbitMQUrl =
    `amqp://${encodeURIComponent(env.RABBITMQ_USER)}` +
    `:${encodeURIComponent(env.RABBITMQ_PASSWORD)}` +
    `@${env.RABBITMQ_HOST}:${env.RABBITMQ_PORT}`;

  let connection:
    | Awaited<ReturnType<typeof createRabbitMQConnection>>
    | undefined;

  let channel:
    | Awaited<ReturnType<typeof createRabbitMQConfirmChannel>>
    | undefined;

  try {
    connection = await createRabbitMQConnection(rabbitMQUrl);

    channel = await createRabbitMQConfirmChannel(connection);

    await assertTaskQueue(channel);

    console.log('TaskFlow dispatcher started.');
    console.log(`Dispatcher ID: ${dispatcherId}`);
    console.log('RabbitMQ connection established.');
    console.log('Job queue is ready.');

    while (!isShuttingDown) {
      try {
        const publishedCount = await publishPendingOutboxEvents(
          channel,
          dispatcherId,
          env.DISPATCHER_BATCH_SIZE,
        );

        if (publishedCount > 0) {
          console.log(
            `Published ${publishedCount} outbox event(s).`,
          );
        }
      } catch (error) {
        console.error('Dispatcher polling failed:', error);
      }

      if (!isShuttingDown) {
        await sleep(env.DISPATCHER_POLL_INTERVAL_MS);
      }
    }
  } finally {
    console.log('Shutting down dispatcher...');

    if (channel) {
      await channel.close().catch(console.error);
    }

    if (connection) {
      await connection.close().catch(console.error);
    }

    await closeDatabaseConnection();

    console.log('Dispatcher stopped.');
  }
}

process.on('SIGINT', () => {
  console.log('Received SIGINT.');
  isShuttingDown = true;
});

process.on('SIGTERM', () => {
  console.log('Received SIGTERM.');
  isShuttingDown = true;
});

main().catch((error: unknown) => {
  console.error('Dispatcher startup failed:', error);
  process.exitCode = 1;
});
