
import {
    assertTaskQueue,
    consumeJobs,
    createRabbitMQChannel,
    createRabbitMQConfirmChannel,
    createRabbitMQConnection,
    DEAD_LETTER_QUEUE,
    publishJob,
    RETRY_DELAY_MS,
    RETRY_QUEUE,
} from '../src/index.js';

const username = process.env.RABBITMQ_USER;
const password = process.env.RABBITMQ_PASSWORD;

if (!username || !password) {
    throw new Error('RabbitMQ credentials are missing');
}

const url =
    `amqp://${encodeURIComponent(username)}` +
    `:${encodeURIComponent(password)}@127.0.0.1:5672`;

const TEST_JOB_ID = '22222222-2222-4222-8222-222222222222';

const sleep = (ms: number) =>
    new Promise<void>((resolve) => setTimeout(resolve, ms));

async function main(): Promise<void> {
    const connection = await createRabbitMQConnection(url);

    const consumerChannel = await createRabbitMQChannel(connection);
    const publisherChannel =
        await createRabbitMQConfirmChannel(connection);
    const retryPublisherChannel =
        await createRabbitMQConfirmChannel(connection);

    let consumer:
        | Awaited<ReturnType<typeof consumeJobs>>
        | undefined;

    try {
        await assertTaskQueue(consumerChannel);
        await assertTaskQueue(publisherChannel);
        await assertTaskQueue(retryPublisherChannel);

        console.log('RabbitMQ retry test infrastructure ready.');

        let handlerAttempts = 0;
        const executionTimes: number[] = [];

        let resolveCompleted!: () => void;

        const completed = new Promise<void>((resolve) => {
            resolveCompleted = resolve;
        });

        consumer = await consumeJobs(
            consumerChannel,
            retryPublisherChannel,
            async (message) => {
                if (message.jobId !== TEST_JOB_ID) {
                    throw new Error(`Unexpected test job: ${message.jobId}`);
                }

                handlerAttempts++;
                executionTimes.push(Date.now());

                console.log(`Handler execution: ${handlerAttempts}`);

                if (handlerAttempts < 3) {
                    throw new Error('Simulated infrastructure failure');
                }

                console.log('Handler succeeded on the third execution.');
                resolveCompleted();
            },
            1,
            () => {
                throw new Error(
                    'Unexpected fatal error during messaging integration test',
                );
            },
        );

        await publishJob(publisherChannel, {
            jobId: TEST_JOB_ID,
            jobType: 'retry_integration_test',
            priority: 0,
        });

        console.log('Test job published.');

        let timeout: ReturnType<typeof setTimeout> | undefined;

        try {
            await Promise.race([
                completed,
                new Promise<never>((_, reject) => {
                    timeout = setTimeout(
                        () => reject(new Error('Retry test timed out')),
                        30000,
                    );
                }),
            ]);
        } finally {
            if (timeout) clearTimeout(timeout);
        }

        await consumer.stopReceiving();
        await consumer.waitForInFlight();

        if (handlerAttempts !== 3) {
            throw new Error(
                `Expected 3 handler executions, received ${handlerAttempts}`,
            );
        }
        for (let i = 1; i < executionTimes.length; i++) {
            const previousTime = executionTimes[i - 1];
            const currentTime = executionTimes[i];

            if (
                previousTime === undefined ||
                currentTime === undefined
            ) {
                throw new Error('Missing handler execution timestamp');
            }

            const delay = currentTime - previousTime;

            console.log(`Retry delay ${i}: ${delay}ms`);

            if (delay < RETRY_DELAY_MS - 250) {
                throw new Error(
                    `Retry occurred too early: expected approximately ${RETRY_DELAY_MS}ms, received ${delay}ms`,
                );
            }
        }

        console.log('Delayed retry test passed.');
        console.log('Total handler executions:', handlerAttempts);

        const retryQueueState = await consumerChannel.checkQueue(
            RETRY_QUEUE,
        );

        console.log('Retry queue state:', retryQueueState);

        console.log('Infrastructure retry integration test passed.');
    } finally {
        if (consumer) {
            await consumer.stop();
        }

        await consumerChannel.close();
        await publisherChannel.close();
        await retryPublisherChannel.close();
        await connection.close();
    }
}

main().catch((error: unknown) => {
    console.error('Retry integration test failed:', error);
    process.exitCode = 1;
});
