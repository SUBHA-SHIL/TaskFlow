import {
  createRabbitMQChannel,
  createRabbitMQConfirmChannel,
  createRabbitMQConnection,
} from '../src/index.js';

import { randomUUID } from 'node:crypto';

const username = process.env.RABBITMQ_USER;
const password = process.env.RABBITMQ_PASSWORD;

if (!username || !password) {
  throw new Error('RabbitMQ credentials are missing');
}

const url =
  `amqp://${encodeURIComponent(username)}` +
  `:${encodeURIComponent(password)}@127.0.0.1:5672`;

const exchange = `taskflow.priority.test.${randomUUID()}`;
const queue = `${exchange}.queue`;
const routingKey = 'priority.test';

const expectedPriorities = [10, 7, 3, 1];

async function main(): Promise<void> {
  const connection = await createRabbitMQConnection(url);
  const consumerChannel = await createRabbitMQChannel(connection);
  const publisherChannel =
    await createRabbitMQConfirmChannel(connection);

  try {
    await consumerChannel.assertExchange(exchange, 'direct', {
      durable: false,
      autoDelete: true,
    });

    await consumerChannel.assertQueue(queue, {
      durable: false,
      exclusive: true,
      autoDelete: true,
      arguments: {
        'x-max-priority': 10,
      },
    });

    await consumerChannel.bindQueue(queue, exchange, routingKey);

    console.log('Temporary priority queue created.');

    // Publish low-to-high so we can verify that higher priorities
    // overtake lower priorities already waiting in the queue.
    for (const priority of [1, 3, 7, 10]) {
      const accepted = publisherChannel.publish(
        exchange,
        routingKey,
        Buffer.from(JSON.stringify({ priority })),
        {
          persistent: false,
          contentType: 'application/json',
          priority,
        },
      );

      if (!accepted) {
        throw new Error('Publisher channel backpressure detected');
      }
    }

    await publisherChannel.waitForConfirms();

    console.log('All four messages published and confirmed.');

    await consumerChannel.prefetch(1);

    const receivedPriorities: number[] = [];

    let resolveCompleted!: () => void;

    const completed = new Promise<void>((resolve) => {
      resolveCompleted = resolve;
    });

    const consumer = await consumerChannel.consume(
      queue,
      (message) => {
        if (!message) return;

        try {
          const payload = JSON.parse(
            message.content.toString(),
          ) as { priority: number };

          receivedPriorities.push(payload.priority);

          console.log('Received priority:', payload.priority);

          consumerChannel.ack(message);

          if (receivedPriorities.length === 4) {
            resolveCompleted();
          }
        } catch (error) {
          consumerChannel.nack(message, false, false);
          throw error;
        }
      },
      { noAck: false },
    );

    let timeout: ReturnType<typeof setTimeout> | undefined;

    try {
      await Promise.race([
        completed,
        new Promise<never>((_, reject) => {
          timeout = setTimeout(
            () => reject(
              new Error('Priority delivery test timed out'),
            ),
            10000,
          );
        }),
      ]);
    } finally {
      if (timeout) clearTimeout(timeout);
      await consumerChannel.cancel(consumer.consumerTag);
    }

    console.log('Expected:', expectedPriorities);
    console.log('Received:', receivedPriorities);

    if (
      JSON.stringify(receivedPriorities) !==
      JSON.stringify(expectedPriorities)
    ) {
      throw new Error('RabbitMQ priority ordering test failed');
    }

    console.log('Priority ordering integration test PASSED.');
  } finally {
    try {
      await consumerChannel.deleteQueue(queue);
      await consumerChannel.deleteExchange(exchange);
    } finally {
      await consumerChannel.close();
      await publisherChannel.close();
      await connection.close();
    }
  }
}

main().catch((error: unknown) => {
  console.error('Priority integration test failed:', error);
  process.exitCode = 1;
});