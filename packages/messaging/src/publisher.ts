
import type { ConfirmChannel, Options } from 'amqplib';

import { TASK_QUEUE } from './queue.js';

export interface JobMessage {
  jobId: string;
  jobType: string;
}

export async function publishJob(
  channel: ConfirmChannel,
  message: JobMessage,
): Promise<void> {
  const payload = Buffer.from(JSON.stringify(message));

  channel.sendToQueue(TASK_QUEUE, payload, {
    persistent: true,
    contentType: 'application/json',
    deliveryMode: 2,
    messageId: message.jobId,
    timestamp: Date.now(),
  } satisfies Options.Publish);

  await channel.waitForConfirms();
}
