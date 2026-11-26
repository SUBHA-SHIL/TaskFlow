import type { Channel } from 'amqplib';

export const TASK_EXCHANGE = 'taskflow.jobs.exchange';
export const TASK_QUEUE = 'taskflow.jobs';
export const TASK_ROUTING_KEY = 'taskflow.jobs';

export const RETRY_EXCHANGE = 'taskflow.jobs.retry.exchange';
export const RETRY_QUEUE = 'taskflow.jobs.retry';
export const RETRY_ROUTING_KEY = 'taskflow.jobs.retry';
export const MAX_INFRASTRUCTURE_RETRIES = 5;

export const RETRY_DELAY_MS = 5000;

export const DEAD_LETTER_EXCHANGE = 'taskflow.jobs.dlx';
export const DEAD_LETTER_QUEUE = 'taskflow.jobs.dead';
export const DEAD_LETTER_ROUTING_KEY = 'taskflow.jobs.dead';

export async function assertTaskQueue(
  channel: Channel,
): Promise<void> {
  // Main exchange.
  await channel.assertExchange(TASK_EXCHANGE, 'direct', {
    durable: true,
  });

  // Retry exchange.
  await channel.assertExchange(RETRY_EXCHANGE, 'direct', {
    durable: true,
  });

  // Dead-letter exchange.
  await channel.assertExchange(DEAD_LETTER_EXCHANGE, 'direct', {
    durable: true,
  });

  // Dead-letter queue.
  await channel.assertQueue(DEAD_LETTER_QUEUE, {
    durable: true,
  });

  await channel.bindQueue(
    DEAD_LETTER_QUEUE,
    DEAD_LETTER_EXCHANGE,
    DEAD_LETTER_ROUTING_KEY,
  );

  // Retry queue: expired messages return to the main exchange.
  await channel.assertQueue(RETRY_QUEUE, {
    durable: true,
    arguments: {
      'x-message-ttl': RETRY_DELAY_MS,
      'x-dead-letter-exchange': TASK_EXCHANGE,
      'x-dead-letter-routing-key': TASK_ROUTING_KEY,
    },
  });

  await channel.bindQueue(
    RETRY_QUEUE,
    RETRY_EXCHANGE,
    RETRY_ROUTING_KEY,
  );

  // Main queue.
  await channel.assertQueue(TASK_QUEUE, {
    durable: true,
    arguments: {
      'x-max-priority': 10,
      'x-dead-letter-exchange': DEAD_LETTER_EXCHANGE,
      'x-dead-letter-routing-key': DEAD_LETTER_ROUTING_KEY,
    },
  });

  await channel.bindQueue(
    TASK_QUEUE,
    TASK_EXCHANGE,
    TASK_ROUTING_KEY,
  );
}
