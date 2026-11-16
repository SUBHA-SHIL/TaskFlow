import type { ChannelModel, ConfirmChannel } from 'amqplib';

export async function createRabbitMQConfirmChannel(
  connection: ChannelModel,
): Promise<ConfirmChannel> {
  const channel = await connection.createConfirmChannel();

  channel.on('error', (error) => {
    console.error('RabbitMQ confirm channel error:', error);
  });

  return channel;
}
