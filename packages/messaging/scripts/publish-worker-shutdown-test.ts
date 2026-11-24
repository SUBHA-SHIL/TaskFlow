import {
  createRabbitMQConfirmChannel,
  createRabbitMQConnection,
  publishJob,
} from '../src/index.js';

const username = process.env.RABBITMQ_USER;
const password = process.env.RABBITMQ_PASSWORD;

if (!username || !password) {
  throw new Error('RabbitMQ credentials are missing.');
}

const url =
  `amqp://${encodeURIComponent(username)}` +
  `:${encodeURIComponent(password)}@127.0.0.1:5672`;

const connection = await createRabbitMQConnection(url);
const channel = await createRabbitMQConfirmChannel(connection);

try {
  await publishJob(channel, {
    jobId: '44444444-4444-4444-8444-444444444444',
    jobType: 'retry_publisher_failure_test',
  });

  await channel.waitForConfirms();

  console.log('Worker shutdown test job published successfully.');
} finally {
  await channel.close();
  await connection.close();
}
