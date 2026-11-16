import type { Channel } from 'amqplib';

export const TASK_QUEUE = 'taskflow.jobs';

export async function assertTaskQueue(
  channel: Channel,
): Promise<void> {
  await channel.assertQueue(TASK_QUEUE, {
    durable: true,
  });
}
