import * as amqp from 'amqplib';
import type { ChannelModel } from 'amqplib';

export const RABBITMQ_HEARTBEAT_SECONDS = 30;

export async function createRabbitMQConnection(
  url: string,
): Promise<ChannelModel> {
  if (!url.trim()) {
    throw new Error('RabbitMQ connection URL is required');
  }

  const separator = url.includes('?') ? '&' : '?';

  const connectionUrl =
    `${url}${separator}heartbeat=${RABBITMQ_HEARTBEAT_SECONDS}`;

  const connection = await amqp.connect(connectionUrl);

  connection.on('error', (error) => {
    console.error('RabbitMQ connection error:', error);
  });

  return connection;
}
