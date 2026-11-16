import * as amqp from 'amqplib';
import type { ChannelModel } from 'amqplib';

export async function createRabbitMQConnection(
  url: string,
): Promise<ChannelModel> {
  if (!url.trim()) {
    throw new Error('RabbitMQ connection URL is required');
  }

  const connection = await amqp.connect(url);

  connection.on('error', (error) => {
    console.error('RabbitMQ connection error:', error);
  });

  return connection;
}
