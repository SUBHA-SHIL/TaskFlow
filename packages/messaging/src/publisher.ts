
import type { ConfirmChannel, Options } from 'amqplib';

import {
  TASK_EXCHANGE,
  TASK_ROUTING_KEY,
} from './queue.js';

export interface JobMessage {
  jobId: string;
  jobType: string;
}

function waitForDrain(channel: ConfirmChannel): Promise<void> {
  return new Promise((resolve, reject) => {
    function cleanup(): void {
      channel.removeListener('drain', onDrain);
      channel.removeListener('error', onError);
      channel.removeListener('close', onClose);
    }

    function onDrain(): void {
      cleanup();
      resolve();
    }

    function onError(error: Error): void {
      cleanup();
      reject(error);
    }

    function onClose(): void {
      cleanup();
      reject(new Error('RabbitMQ channel closed before drain'));
    }

    channel.once('drain', onDrain);
    channel.once('error', onError);
    channel.once('close', onClose);
  });
}

export async function publishJob(
  channel: ConfirmChannel,
  message: JobMessage,
): Promise<void> {
  const payload = Buffer.from(JSON.stringify(message));

  const canContinue = channel.publish(
    TASK_EXCHANGE,
    TASK_ROUTING_KEY,
    payload,
    {
      persistent: true,
      contentType: 'application/json',
      deliveryMode: 2,
      messageId: message.jobId,
      timestamp: Date.now(),
    } satisfies Options.Publish,
  );

  if (!canContinue) {
    await waitForDrain(channel);
  }

  await channel.waitForConfirms();
}
