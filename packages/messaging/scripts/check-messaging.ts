import {
  assertTaskQueue,
  consumeJobs,
  createRabbitMQChannel,
  createRabbitMQConfirmChannel,
  createRabbitMQConnection,
  publishJob,
} from '../src/index.js';

const username = process.env.RABBITMQ_USER;
const password = process.env.RABBITMQ_PASSWORD;

if (!username || !password) {
  throw new Error('RabbitMQ credentials are missing');
}

const url = `amqp://${encodeURIComponent(username)}:${encodeURIComponent(password)}@127.0.0.1:5672`;

async function main(): Promise<void> {
  const connection = await createRabbitMQConnection(url);

  const consumerChannel = await createRabbitMQChannel(connection);
  const publisherChannel =
    await createRabbitMQConfirmChannel(connection);

  try {
    await assertTaskQueue(consumerChannel);
    await assertTaskQueue(publisherChannel);

    console.log('RabbitMQ connection and queue ready.');

    let resolveProcessed!: () => void;

    const processed = new Promise<void>((resolve) => {
      resolveProcessed = resolve;
    });

    await consumeJobs(consumerChannel, async (message) => {
      console.log('Message received:', message);
      resolveProcessed();
    });

    await publishJob(publisherChannel, {
      jobId: '11111111-1111-4111-8111-111111111111',
      jobType: 'messaging_test',
    });

    console.log('Message published and broker confirmed.');

    let timeout: ReturnType<typeof setTimeout> | undefined;

    try {
      await Promise.race([
        processed,
        new Promise<never>((_, reject) => {
          timeout = setTimeout(
            () => reject(new Error('Message processing timed out')),
            10000,
          );
        }),
      ]);
    } finally {
      if (timeout) clearTimeout(timeout);
    }

    // Allow the consumer callback to acknowledge the message.
    await new Promise((resolve) => setTimeout(resolve, 100));

    console.log('Messaging integration test passed.');
  } finally {
    await consumerChannel.close();
    await publisherChannel.close();
    await connection.close();
  }
}

main().catch((error: unknown) => {
  console.error('Messaging integration test failed:', error);
  process.exitCode = 1;
});
