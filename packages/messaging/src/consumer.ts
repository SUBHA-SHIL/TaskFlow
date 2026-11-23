
import type {
  Channel,
  ConfirmChannel,
  ConsumeMessage,
} from 'amqplib';

import {
  MAX_INFRASTRUCTURE_RETRIES,
  TASK_QUEUE,
} from './queue.js';

import { publishRetry } from './retry-publisher.js';
import type { JobMessage } from './publisher.js';

export type JobMessageHandler = (
  message: JobMessage,
) => Promise<void>;

export type JobConsumerFatalErrorHandler = (
  error: unknown,
) => void;

const RETRY_COUNT_HEADER = 'x-taskflow-retry-count';

function getRetryCount(rawMessage: ConsumeMessage): number | null {
  const value =
    rawMessage.properties.headers?.[RETRY_COUNT_HEADER];

  if (value === undefined) {
    return 0;
  }

  if (
    typeof value !== 'number' ||
    !Number.isSafeInteger(value) ||
    value < 0
  ) {
    return null;
  }

  return value;
}

export interface JobConsumer {
  stopReceiving(): Promise<void>;
  waitForInFlight(): Promise<void>;
  stop(): Promise<void>;
}

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
  retryPublisherChannel: ConfirmChannel,
  handler: JobMessageHandler,
  concurrency: number,
  onFatalError: JobConsumerFatalErrorHandler,
): Promise<JobConsumer> {
  if (!Number.isSafeInteger(concurrency) || concurrency <= 0) {
    throw new Error('Consumer concurrency must be a positive integer');
  }

  const inFlight = new Set<Promise<void>>();

  await channel.prefetch(concurrency);

  const consumer = await channel.consume(
    TASK_QUEUE,
    (rawMessage: ConsumeMessage | null) => {
      if (!rawMessage) {
        return;
      }

      const processing = processMessage(rawMessage)
        .catch((error) => {
          console.error(
            'Unexpected consumer processing error:',
            error,
          );
        })
        .finally(() => {
          inFlight.delete(processing);
        });

      inFlight.add(processing);
    },
  );

  async function processMessage(
    rawMessage: ConsumeMessage,
  ): Promise<void> {
    let message: JobMessage;

    try {
      message = parseJobMessage(rawMessage);
    } catch (error) {
      console.error('Invalid job message:', error);

      channel.nack(rawMessage, false, false);
      return;
    }

    try {
      await handler(message);

      channel.ack(rawMessage);
    } catch (error) {
      console.error(
        `Infrastructure error while processing job ${message.jobId}:`,
        error,
      );

      const retryCount = getRetryCount(rawMessage);

      if (retryCount === null) {
        console.error(
          `Invalid infrastructure retry-count header for job ${message.jobId}. Sending to dead-letter queue.`,
        );

        channel.nack(rawMessage, false, false);
        return;
      }

      if (retryCount >= MAX_INFRASTRUCTURE_RETRIES) {
        console.error(
          `Maximum infrastructure retries reached for job ${message.jobId}. Sending to dead-letter queue.`,
        );

        channel.nack(rawMessage, false, false);
        return;
      }

      try {
        await publishRetry(
          retryPublisherChannel,
          message,
          retryCount + 1,
        );

        channel.ack(rawMessage);

        console.log(
          `Job ${message.jobId} scheduled for infrastructure retry ${retryCount + 1}/${MAX_INFRASTRUCTURE_RETRIES}.`,
        );
      } catch (retryError) {
        console.error(
          `Failed to publish retry for job ${message.jobId}. Leaving original message unacknowledged.`,
          retryError,
        );

        try {
          onFatalError(retryError);
        } catch (callbackError) {
          console.error(
            'Fatal-error callback failed:',
            callbackError,
          );
        }
      }
    }
  }

  let stopReceivingPromise: Promise<void> | null = null;

  async function stopReceiving(): Promise<void> {
    if (!stopReceivingPromise) {
      stopReceivingPromise = channel
        .cancel(consumer.consumerTag)
        .then(() => undefined);
    }

    await stopReceivingPromise;
  }

  async function waitForInFlight(): Promise<void> {
    await Promise.all([...inFlight]);
  }

  return {
    stopReceiving,
    waitForInFlight,
    async stop(): Promise<void> {
      await stopReceiving();
      await waitForInFlight();

      console.log('RabbitMQ consumer stopped.', {
        consumerTag: consumer.consumerTag,
      });
    },
  };
}
