
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

const INITIAL_RECONNECT_DELAY_MS = 1_000;
const MAX_RECONNECT_DELAY_MS = 30_000;

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

  let connectionFailed = false;
  let channelFailed = false;
  let reconnectAttempts = 0;

  async function closeRabbitMQ(): Promise<void> {
    const currentChannel = channel;
    const currentConnection = connection;

    channel = undefined;
    connection = undefined;

    if (currentChannel) {
      await currentChannel.close().catch(() => undefined);
    }

    if (currentConnection) {
      await currentConnection.close().catch(() => undefined);
    }

    connectionFailed = false;
    channelFailed = false;
  }

  async function connectRabbitMQ(): Promise<void> {
    connection = await createRabbitMQConnection(rabbitMQUrl);

    connection.on('error', (error) => {
      console.error('Dispatcher RabbitMQ connection error:', error);
      connectionFailed = true;
    });

    connection.on('close', () => {
      console.error('Dispatcher RabbitMQ connection closed.');
      connectionFailed = true;
    });

    channel = await createRabbitMQConfirmChannel(connection);

    channel.on('error', (error) => {
      console.error('Dispatcher RabbitMQ channel error:', error);
      channelFailed = true;
    });

    channel.on('close', () => {
      console.error('Dispatcher RabbitMQ channel closed.');
      channelFailed = true;
    });

    await assertTaskQueue(channel);

    connectionFailed = false;
    channelFailed = false;
    reconnectAttempts = 0;

    console.log('RabbitMQ connection established.');
    console.log('Job queue is ready.');
  }

  try {
    console.log('TaskFlow dispatcher started.');
    console.log(`Dispatcher ID: ${dispatcherId}`);

    while (!isShuttingDown) {
      if (!connection || !channel || connectionFailed || channelFailed) {
        await closeRabbitMQ();

        if (isShuttingDown) {
          break;
        }

        try {
          await connectRabbitMQ();
        } catch (error) {
          await closeRabbitMQ();

          reconnectAttempts++;

          const delay = Math.min(
            INITIAL_RECONNECT_DELAY_MS *
              2 ** Math.min(reconnectAttempts - 1, 5),
            MAX_RECONNECT_DELAY_MS,
          );

          console.error('RabbitMQ connection failed:', error);
          console.log(`Retrying RabbitMQ connection in ${delay} ms.`);

          await sleep(delay);
          continue;
        }
      }

      if (isShuttingDown) {
        break;
      }

      try {
        const publishedCount = await publishPendingOutboxEvents(
          channel!,
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

    await closeRabbitMQ();
    await closeDatabaseConnection();

    console.log('Dispatcher stopped.');
  }
}

process.once('SIGINT', () => {
  console.log('Received SIGINT.');
  isShuttingDown = true;
});

process.once('SIGTERM', () => {
  console.log('Received SIGTERM.');
  isShuttingDown = true;
});

main().catch((error: unknown) => {
  console.error('Dispatcher startup failed:', error);
  process.exitCode = 1;
});
