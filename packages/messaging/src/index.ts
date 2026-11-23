export { createRabbitMQConnection } from './connection.js';
export { createRabbitMQChannel } from './channel.js';
export { createRabbitMQConfirmChannel } from './confirm-channel.js';
export {
  assertTaskQueue,
  TASK_QUEUE,
  TASK_EXCHANGE,
  TASK_ROUTING_KEY,
  DEAD_LETTER_EXCHANGE,
  DEAD_LETTER_QUEUE,
  DEAD_LETTER_ROUTING_KEY,
  RETRY_EXCHANGE,
  RETRY_QUEUE,
  RETRY_ROUTING_KEY,
  RETRY_DELAY_MS,
  MAX_INFRASTRUCTURE_RETRIES
} from './queue.js';
export { publishJob } from './publisher.js';
export { consumeJobs } from './consumer.js';
export { publishRetry } from './retry-publisher.js';

export type { JobMessage } from './publisher.js';

export type {
  JobMessageHandler,
  JobConsumer,
} from './consumer.js';