
import {
  assertTaskQueue,
  createRabbitMQChannel,
  createRabbitMQConfirmChannel,
  createRabbitMQConnection,
  publishRetry,
  RETRY_QUEUE,
  TASK_QUEUE,
  type JobMessage,
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

const EXPECTED_PRIORITIES = [10, 5, 1];
const RETRY_COUNT = 1;
const TIMEOUT_MS = 20000;

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function main(): Promise<void> {
  const connection = await createRabbitMQConnection(url);

  const setupChannel = await createRabbitMQChannel(connection);
  const publisherChannel =
    await createRabbitMQConfirmChannel(connection);
  const consumerChannel = await createRabbitMQChannel(connection);

  const testJobIds = new Set<string>();

  let consumerTag: string | undefined;
  let timedOut = false;

  try {
    await assertTaskQueue(setupChannel);
    await assertTaskQueue(publisherChannel);
    await assertTaskQueue(consumerChannel);

    const mainQueueState = await setupChannel.checkQueue(TASK_QUEUE);
    const retryQueueState = await setupChannel.checkQueue(RETRY_QUEUE);

    if (
      mainQueueState.messageCount !== 0 ||
      mainQueueState.consumerCount !== 0 ||
      retryQueueState.messageCount !== 0 ||
      retryQueueState.consumerCount !== 0
    ) {
      throw new Error(
        'Precondition failed: main and retry queues must be empty and have no consumers.',
      );
    }

    console.log('RabbitMQ queues are empty and ready.');

    const messages: JobMessage[] = [1, 5, 10].map((priority) => {
      const jobId = randomUUID();
      testJobIds.add(jobId);

      return {
        jobId,
        jobType: 'retry_priority_test',
        priority,
      };
    });

    for (const message of messages) {
      await publishRetry(
        publisherChannel,
        message,
        RETRY_COUNT,
      );

      console.log('Published retry message:', {
        jobId: message.jobId,
        priority: message.priority,
        retryCount: RETRY_COUNT,
      });
    }

    console.log('Waiting for retry messages to expire...');

    await sleep(5500);

    const receivedPriorities: number[] = [];
    const receivedJobIds = new Set<string>();

    await consumerChannel.prefetch(1);

    const result = await consumerChannel.consume(
      TASK_QUEUE,
      (message) => {
        if (!message) {
          return;
        }

        try {
          const jobId = message.properties.messageId;
          const payload = JSON.parse(
            message.content.toString(),
          ) as JobMessage;

          if (
            !jobId ||
            !testJobIds.has(jobId) ||
            payload.jobId !== jobId
          ) {
            console.error(
              'Unexpected message received. Leaving it unacknowledged.',
              { jobId },
            );

            void consumerChannel.cancel(result.consumerTag);
            return;
          }

          if (receivedJobIds.has(jobId)) {
            console.error('Duplicate test message received:', jobId);
            void consumerChannel.cancel(result.consumerTag);
            return;
          }

          const priority = message.properties.priority;
          const retryCount =
            message.properties.headers?.[
              'x-taskflow-retry-count'
            ];

          if (
            priority !== payload.priority ||
            retryCount !== RETRY_COUNT
          ) {
            console.error('Message metadata mismatch:', {
              jobId,
              brokerPriority: priority,
              payloadPriority: payload.priority,
              retryCount,
            });

            void consumerChannel.cancel(result.consumerTag);
            return;
          }

          receivedPriorities.push(priority);
          receivedJobIds.add(jobId);

          console.log('Received retried message:', {
            jobId,
            priority,
            retryCount,
          });

          consumerChannel.ack(message);

          if (receivedJobIds.size === messages.length) {
            void consumerChannel.cancel(result.consumerTag);
          }
        } catch (error) {
          console.error('Failed to process received message:', error);
          void consumerChannel.cancel(result.consumerTag);
        }
      },
      { noAck: false },
    );

    consumerTag = result.consumerTag;

    const deadline = Date.now() + TIMEOUT_MS;

    while (
      receivedJobIds.size < messages.length &&
      Date.now() < deadline
    ) {
      await sleep(100);
    }

    timedOut = receivedJobIds.size !== messages.length;

    await consumerChannel.cancel(consumerTag);
    consumerTag = undefined;

    if (timedOut) {
      throw new Error(
        `Timed out: received ${receivedJobIds.size}/${messages.length} test messages.`,
      );
    }

    console.log('Expected priority order:', EXPECTED_PRIORITIES);
    console.log('Actual priority order:', receivedPriorities);

    if (
      JSON.stringify(receivedPriorities) !==
      JSON.stringify(EXPECTED_PRIORITIES)
    ) {
      throw new Error('Retry priority ordering failed.');
    }

    const finalMainQueue = await setupChannel.checkQueue(TASK_QUEUE);
    const finalRetryQueue = await setupChannel.checkQueue(RETRY_QUEUE);

    if (
      finalMainQueue.messageCount !== 0 ||
      finalRetryQueue.messageCount !== 0
    ) {
      throw new Error(
        'Test queues are not empty after processing test messages.',
      );
    }

    console.log('All retry messages preserved their priority.');
    console.log('Retry priority integration test PASSED.');
  } finally {
    if (consumerTag) {
      await consumerChannel.cancel(consumerTag).catch(() => undefined);
    }

    await consumerChannel.close().catch(() => undefined);
    await publisherChannel.close().catch(() => undefined);
    await setupChannel.close().catch(() => undefined);
    await connection.close().catch(() => undefined);
  }
}

main().catch((error: unknown) => {
  console.error('Retry priority integration test failed:', error);
  process.exitCode = 1;
});
