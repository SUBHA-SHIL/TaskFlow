
import type {
  Channel,
  ConsumeMessage,
} from 'amqplib';

import { TASK_QUEUE } from './queue.js';
import type { JobMessage } from './publisher.js';

export type JobMessageHandler = (
  message: JobMessage,
) => Promise<void>;

function parseJobMessage(rawMessage: ConsumeMessage): JobMessage {
  const message: unknown = JSON.parse(
    rawMessage.content.toString(),
  );

  if (
    typeof message !== 'object' ||
    message === null ||
    !('jobId' in message) ||
    !('jobType' in message) ||
    typeof message.jobId !== 'string' ||
    typeof message.jobType !== 'string' ||
    !message.jobId.trim() ||
    !message.jobType.trim()
  ) {
    throw new Error('Invalid job message payload');
  }

  return {
    jobId: message.jobId,
    jobType: message.jobType,
  };
}

export async function consumeJobs(
  channel: Channel,
  handler: JobMessageHandler,
  concurrency = 1,
): Promise<void> {
  if (!Number.isSafeInteger(concurrency) || concurrency <= 0) {
    throw new Error('Consumer concurrency must be a positive integer');
  }

  await channel.prefetch(concurrency);

  await channel.consume(
    TASK_QUEUE,
    async (rawMessage: ConsumeMessage | null) => {
      if (!rawMessage) {
        return;
      }

      let message: JobMessage;

      try {
        message = parseJobMessage(rawMessage);
      } catch (error) {
        console.error('Invalid job message:', error);

        // Invalid messages cannot be fixed by redelivery.
        channel.nack(rawMessage, false, false);
        return;
      }

      try {
        await handler(message);

        // The handler completed its processing successfully.
        channel.ack(rawMessage);
      } catch (error) {
        console.error(
          `Infrastructure error while processing job ${message.jobId}:`,
          error,
        );

        // Retry infrastructure failures through RabbitMQ.
        channel.nack(rawMessage, false, true);
      }
    },
  );
}
