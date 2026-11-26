import type { ConfirmChannel, Options } from 'amqplib';

import {
    RETRY_EXCHANGE,
    RETRY_ROUTING_KEY,
} from './queue.js';

import type { JobMessage } from './publisher.js';

export async function publishRetry(
    channel: ConfirmChannel,
    message: JobMessage,
    retryCount: number,
): Promise<void> {
    const payload = Buffer.from(JSON.stringify(message));

    const canContinue = channel.publish(
        RETRY_EXCHANGE,
        RETRY_ROUTING_KEY,
        payload,
        {
            persistent: true,
            contentType: 'application/json',
            deliveryMode: 2,
            messageId: message.jobId,
            timestamp: Date.now(),
            priority: message.priority,
            headers: {
                'x-taskflow-retry-count': retryCount,
            },
        } satisfies Options.Publish,
    );

    if (!canContinue) {
        await new Promise<void>((resolve, reject) => {
            function cleanup(): void {
                channel.removeListener('drain', onDrain);
                channel.removeListener('error', onError);
                channel.removeListener('close', onClose);
            }

            function onDrain(): void {
                cleanup();
                resolve();
            }

            function onError(error: Error): void {
                cleanup();
                reject(error);
            }

            function onClose(): void {
                cleanup();
                reject(
                    new Error('RabbitMQ channel closed before drain'),
                );
            }

            channel.once('drain', onDrain);
            channel.once('error', onError);
            channel.once('close', onClose);
        });
    }

    await channel.waitForConfirms();
}
