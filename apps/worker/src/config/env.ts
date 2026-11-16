
import {
  getPositiveInteger,
  getRequiredEnv,
} from '@taskflow/config';

const RABBITMQ_USER = getRequiredEnv('RABBITMQ_USER');
const RABBITMQ_PASSWORD = getRequiredEnv('RABBITMQ_PASSWORD');

const RABBITMQ_HOST = process.env.RABBITMQ_HOST || '127.0.0.1';

const RABBITMQ_PORT = getPositiveInteger(
  'RABBITMQ_PORT',
  5672,
);

const WORKER_CONCURRENCY = getPositiveInteger(
  'WORKER_CONCURRENCY',
  5,
);

export const workerEnv = {
  rabbitMQUrl: `amqp://${encodeURIComponent(RABBITMQ_USER)}:${encodeURIComponent(RABBITMQ_PASSWORD)}@${RABBITMQ_HOST}:${RABBITMQ_PORT}`,
  concurrency: WORKER_CONCURRENCY,
};
