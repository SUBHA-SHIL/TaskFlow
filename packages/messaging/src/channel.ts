import type { Channel, ChannelModel } from 'amqplib';

export async function createRabbitMQChannel(
  connection: ChannelModel,
): Promise<Channel> {
  const channel = await connection.createChannel();

  channel.on('error', (error) => {
    console.error('RabbitMQ channel error:', error);
  });

  return channel;
}
