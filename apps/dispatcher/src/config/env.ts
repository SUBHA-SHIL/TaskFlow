
import {
  getPositiveInteger,
  getRequiredEnv,
} from '@taskflow/config';

export const env = {
  RABBITMQ_USER: getRequiredEnv('RABBITMQ_USER'),
  RABBITMQ_PASSWORD: getRequiredEnv('RABBITMQ_PASSWORD'),
  RABBITMQ_HOST: process.env.RABBITMQ_HOST ?? '127.0.0.1',
  RABBITMQ_PORT: getPositiveInteger('RABBITMQ_PORT', 5672),
  DISPATCHER_POLL_INTERVAL_MS: getPositiveInteger(
    'DISPATCHER_POLL_INTERVAL_MS',
    1000,
  ),
  DISPATCHER_BATCH_SIZE: getPositiveInteger(
    'DISPATCHER_BATCH_SIZE',
    10,
  ),
};
