import {
  assertTaskQueue,
  consumeJobs,
  createRabbitMQChannel,
  createRabbitMQConfirmChannel,
  createRabbitMQConnection,
  DEAD_LETTER_QUEUE,
  publishJob,
  TASK_EXCHANGE,
  TASK_ROUTING_KEY,
} from '../src/index.js';

const username = process.env.RABBITMQ_USER;
const password = process.env.RABBITMQ_PASSWORD;

if (!username || !password) {
  throw new Error('RabbitMQ credentials are missing');
}

const url =
  `amqp://${encodeURIComponent(username)}` +
  `:${encodeURIComponent(password)}@127.0.0.1:5672`;

async function main(): Promise<void> {
  const connection = await createRabbitMQConnection(url);

  const consumerChannel = await createRabbitMQChannel(connection);

  const publisherChannel =
    await createRabbitMQConfirmChannel(connection);

  const retryPublisherChannel =
    await createRabbitMQConfirmChannel(connection);

  try {
    await assertTaskQueue(consumerChannel);
    await assertTaskQueue(publisherChannel);
    await assertTaskQueue(retryPublisherChannel);

    console.log('RabbitMQ connection and queue ready.');

    let resolveProcessed!: () => void;

    const processed = new Promise<void>((resolve) => {
      resolveProcessed = resolve;
    });

    await consumeJobs(
      consumerChannel,
      retryPublisherChannel,
      async (message) => {
        console.log('Message received:', message);
        resolveProcessed();
      },
      1,
      () => {
        throw new Error(
          'Unexpected fatal error during messaging integration test',
        );
      },
    );

    await publishJob(publisherChannel, {
      jobId: '11111111-1111-4111-8111-111111111111',
      jobType: 'messaging_test',
    });

    console.log('Valid message published and confirmed.');

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

    await new Promise((resolve) => setTimeout(resolve, 100));

    console.log('Valid message delivery test passed.');

    // Publish an intentionally invalid payload directly.
    publisherChannel.publish(
      TASK_EXCHANGE,
      TASK_ROUTING_KEY,
      Buffer.from('{ invalid JSON'),
      {
        persistent: true,
        contentType: 'application/json',
      },
    );

    await publisherChannel.waitForConfirms();

    console.log('Invalid message published and confirmed.');

    // Wait until the consumer rejects the invalid message.
    const deadline = Date.now() + 10000;

    let deadLetterMessage =
      await consumerChannel.get(DEAD_LETTER_QUEUE, {
        noAck: true,
      });

    while (!deadLetterMessage && Date.now() < deadline) {
      await new Promise((resolve) => setTimeout(resolve, 100));

      deadLetterMessage =
        await consumerChannel.get(DEAD_LETTER_QUEUE, {
          noAck: true,
        });
    }

    if (!deadLetterMessage) {
      throw new Error(
        'Invalid message was not routed to the dead-letter queue',
      );
    }

    console.log(
      'Dead-letter message received:',
      deadLetterMessage.content.toString(),
    );

    const deathHeader = deadLetterMessage.properties.headers?.['x-death'];

    if (!Array.isArray(deathHeader) || deathHeader.length === 0) {
      throw new Error('Dead-letter metadata was not found');
    }

    console.log('Dead-letter metadata verified.');

    console.log('Messaging integration test passed.');
  } finally {
    await consumerChannel.close();
    await publisherChannel.close();
    await retryPublisherChannel.close();
    await connection.close();
  }
}

main().catch((error: unknown) => {
  console.error('Messaging integration test failed:', error);
  process.exitCode = 1;
});
