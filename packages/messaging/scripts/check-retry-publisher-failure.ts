
import {
  assertTaskQueue,
  consumeJobs,
  createRabbitMQChannel,
  createRabbitMQConfirmChannel,
  createRabbitMQConnection,
  publishJob,
  TASK_QUEUE,
} from '../src/index.js';

const username = process.env.RABBITMQ_USER;
const password = process.env.RABBITMQ_PASSWORD;

if (!username || !password) {
  throw new Error('RabbitMQ credentials are missing');
}

const url =
  `amqp://${encodeURIComponent(username)}` +
  `:${encodeURIComponent(password)}@127.0.0.1:5672`;

const TEST_JOB_ID = '44444444-4444-4444-8444-444444444444';

async function main(): Promise<void> {
  const connection = await createRabbitMQConnection(url);

  const consumerChannel = await createRabbitMQChannel(connection);
  const publisherChannel =
    await createRabbitMQConfirmChannel(connection);
  const retryPublisherChannel =
    await createRabbitMQConfirmChannel(connection);

  let consumer:
    | Awaited<ReturnType<typeof consumeJobs>>
    | undefined;

  let consumerChannelClosed = false;
  let retryPublisherChannelClosed = false;

  try {
    await assertTaskQueue(consumerChannel);
    await assertTaskQueue(publisherChannel);
    await assertTaskQueue(retryPublisherChannel);

    const queueState = await consumerChannel.checkQueue(TASK_QUEUE);

    if (queueState.messageCount !== 0) {
      throw new Error(
        `Main queue must be empty before this test. Found ${queueState.messageCount} messages.`,
      );
    }

    let handlerAttempts = 0;
    let resolveFatalError!: () => void;

    const fatalError = new Promise<void>((resolve) => {
      resolveFatalError = resolve;
    });

    consumer = await consumeJobs(
      consumerChannel,
      retryPublisherChannel,
      async () => {
        handlerAttempts++;

        console.log(`Handler execution: ${handlerAttempts}`);

        throw new Error('Simulated infrastructure failure');
      },
      1,
      (error) => {
        console.log('Fatal retry-publishing error received:', error);
        resolveFatalError();
      },
    );

    // Simulate retry publisher failure.
    await retryPublisherChannel.close();
    retryPublisherChannelClosed = true;

    console.log('Retry publisher channel deliberately closed.');

    await publishJob(publisherChannel, {
      jobId: TEST_JOB_ID,
      jobType: 'retry_publisher_failure_test',
    });

    console.log('Test job published.');

    let timeout: ReturnType<typeof setTimeout> | undefined;

    try {
      await Promise.race([
        fatalError,
        new Promise<never>((_, reject) => {
          timeout = setTimeout(
            () => reject(new Error('Fatal-error callback timed out')),
            10000,
          );
        }),
      ]);
    } finally {
      if (timeout) clearTimeout(timeout);
    }

    await consumer.stopReceiving();
    await consumer.waitForInFlight();

    if (handlerAttempts !== 1) {
      throw new Error(
        `Expected one handler execution, received ${handlerAttempts}`,
      );
    }

    console.log('Fatal-error callback verified.');

    // Closing the consumer channel requeues its unacknowledged message.
    await consumerChannel.close();
    consumerChannelClosed = true;

    console.log('Consumer channel closed.');

    const verificationChannel =
      await createRabbitMQChannel(connection);

    try {
      const requeuedMessage = await verificationChannel.get(
        TASK_QUEUE,
        { noAck: true },
      );

      if (!requeuedMessage) {
        throw new Error(
          'Original message was not requeued after consumer channel closure.',
        );
      }

      const payload: unknown = JSON.parse(
        requeuedMessage.content.toString(),
      );

      if (
        typeof payload !== 'object' ||
        payload === null ||
        !('jobId' in payload) ||
        payload.jobId !== TEST_JOB_ID
      ) {
        throw new Error('Unexpected requeued message payload.');
      }

      const retryCount =
        requeuedMessage.properties.headers?.[
          'x-taskflow-retry-count'
        ];

      if (retryCount !== undefined) {
        throw new Error(
          'Original message unexpectedly contains a retry-count header.',
        );
      }

      console.log('Original message successfully requeued.');
      console.log('Original message payload verified.');
      console.log('Original retry-count header verified.');
      console.log('Retry publisher failure integration test passed.');
    } finally {
      await verificationChannel.close();
    }
  } finally {
    if (consumer) {
      try {
        await consumer.stop();
      } catch {
        // The consumer channel may already be closed.
      }
    }

    if (!consumerChannelClosed) {
      try {
        await consumerChannel.close();
      } catch {
        // Channel may already be closed.
      }
    }

    if (!retryPublisherChannelClosed) {
      try {
        await retryPublisherChannel.close();
      } catch {
        // Channel may already be closed.
      }
    }

    try {
      await publisherChannel.close();
    } catch {
      // Channel may already be closed.
    }

    await connection.close();
  }
}

main().catch((error: unknown) => {
  console.error('Retry publisher failure test failed:', error);
  process.exitCode = 1;
});
