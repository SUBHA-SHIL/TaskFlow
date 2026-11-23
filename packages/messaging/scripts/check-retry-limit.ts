
import {
    assertTaskQueue,
    consumeJobs,
    createRabbitMQChannel,
    createRabbitMQConfirmChannel,
    createRabbitMQConnection,
    DEAD_LETTER_QUEUE,
    MAX_INFRASTRUCTURE_RETRIES,
    publishJob,
    RETRY_DELAY_MS,
} from '../src/index.js';

const username = process.env.RABBITMQ_USER;
const password = process.env.RABBITMQ_PASSWORD;

if (!username || !password) {
    throw new Error('RabbitMQ credentials are missing');
}

const url =
    `amqp://${encodeURIComponent(username)}` +
    `:${encodeURIComponent(password)}@127.0.0.1:5672`;

const TEST_JOB_ID = '33333333-3333-4333-8333-333333333333';

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

        const deadLetterState =
            await consumerChannel.checkQueue(DEAD_LETTER_QUEUE);

        if (deadLetterState.messageCount !== 0) {
            throw new Error(
                `Dead-letter queue must be empty before this test. Found ${deadLetterState.messageCount} messages.`,
            );
        }

        console.log('RabbitMQ retry-limit test infrastructure ready.');

        let handlerAttempts = 0;
        let resolveSixthAttempt!: () => void;

        const sixthAttempt = new Promise<void>((resolve) => {
            resolveSixthAttempt = resolve;
        });

        consumer = await consumeJobs(
            consumerChannel,
            retryPublisherChannel,
            async (message) => {
                if (message.jobId !== TEST_JOB_ID) {
                    throw new Error(`Unexpected test job: ${message.jobId}`);
                }

                handlerAttempts++;

                console.log(`Handler execution: ${handlerAttempts}`);

                if (handlerAttempts === MAX_INFRASTRUCTURE_RETRIES + 1) {
                    resolveSixthAttempt();
                }

                throw new Error('Simulated permanent infrastructure failure');
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
            jobType: 'retry_limit_integration_test',
        });

        console.log('Test job published.');

        let timeout: ReturnType<typeof setTimeout> | undefined;

        try {
            await Promise.race([
                sixthAttempt,
                new Promise<never>((_, reject) => {
                    timeout = setTimeout(
                        () => reject(new Error('Maximum retry test timed out')),
                        45000,
                    );
                }),
            ]);
        } finally {
            if (timeout) clearTimeout(timeout);
        }

        await consumer.stopReceiving();
        await consumer.waitForInFlight();

        if (handlerAttempts !== MAX_INFRASTRUCTURE_RETRIES + 1) {
            throw new Error(
                `Expected ${MAX_INFRASTRUCTURE_RETRIES + 1} executions, received ${handlerAttempts}`,
            );
        }

        console.log('Maximum handler execution count verified.');

        let deadLetterMessage = null;
        const deadline = Date.now() + 5000;

        while (!deadLetterMessage && Date.now() < deadline) {
            deadLetterMessage = await consumerChannel.get(
                DEAD_LETTER_QUEUE,
                { noAck: true },
            );

            if (!deadLetterMessage) {
                await sleep(100);
            }
        }

        if (!deadLetterMessage) {
            throw new Error(
                'Message was not routed to the dead-letter queue.',
            );
        }

        const deadLetterPayload: unknown = JSON.parse(
            deadLetterMessage.content.toString(),
        );

        if (
            typeof deadLetterPayload !== 'object' ||
            deadLetterPayload === null ||
            !('jobId' in deadLetterPayload) ||
            deadLetterPayload.jobId !== TEST_JOB_ID
        ) {
            throw new Error('Unexpected dead-letter message payload.');
        }

        const deathHeader =
            deadLetterMessage.properties.headers?.['x-death'];

        if (!Array.isArray(deathHeader) || deathHeader.length === 0) {
            throw new Error('Dead-letter metadata was not found.');
        }

        console.log('Dead-letter payload verified.');
        console.log('Dead-letter metadata verified.');
        console.log('Total handler executions:', handlerAttempts);
        console.log('Retry delay configured:', RETRY_DELAY_MS, 'ms');

        console.log('Maximum infrastructure retry test passed.');
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
    console.error('Retry-limit integration test failed:', error);
    process.exitCode = 1;
});
