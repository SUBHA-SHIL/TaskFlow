
import { randomUUID } from 'node:crypto';

import { pool } from '@taskflow/database';

import {
    createRabbitMQChannel,
    createRabbitMQConfirmChannel,
    createRabbitMQConnection,
    TASK_QUEUE,
} from '@taskflow/messaging';

import { publishPendingOutboxEvents } from '../src/features/outbox-publisher/outbox.service.js';

const priorities = [1, 3, 5, 7, 10];
const expectedPriorities = [...priorities].sort((a, b) => b - a);

const testJobIds = priorities.map(() => randomUUID());
const dispatcherId = `priority-pipeline-test-${randomUUID()}`;

const testJobIdSet = new Set<string>(testJobIds);

const username = process.env.RABBITMQ_USER;
const password = process.env.RABBITMQ_PASSWORD;

if (!username || !password) {
    throw new Error('RabbitMQ credentials are missing.');
}

const rabbitMQUrl =
    `amqp://${encodeURIComponent(username)}:` +
    `${encodeURIComponent(password)}@127.0.0.1:5672`;

let connection: Awaited<ReturnType<typeof createRabbitMQConnection>> | undefined;
let consumerChannel: Awaited<ReturnType<typeof createRabbitMQChannel>> | undefined;
let publisherChannel: Awaited<ReturnType<typeof createRabbitMQConfirmChannel>> | undefined;

let consumerTag: string | undefined;
let insertedJobIds: string[] = [];
let receivedPriorities: number[] = [];
let receivedJobIds: string[] = [];

function assert(condition: boolean, message: string): void {
    if (!condition) {
        throw new Error(message);
    }
}

async function main(): Promise<void> {
    try {
        console.log('Connecting to RabbitMQ...');

        connection = await createRabbitMQConnection(rabbitMQUrl);
        consumerChannel = await createRabbitMQChannel(connection);
        publisherChannel = await createRabbitMQConfirmChannel(connection);

        console.log('RabbitMQ connection established.');

        console.log('Checking production queue...');

        const queueState = await consumerChannel.checkQueue(TASK_QUEUE);

        assert(
            queueState.messageCount === 0,
            `Expected an empty queue, found ${queueState.messageCount} ready messages.`,
        );

        assert(
            queueState.consumerCount === 0,
            `Expected zero consumers, found ${queueState.consumerCount}.`,
        );

        console.log('Production queue is empty and has no consumers.');

        const pendingEvents = await pool.query<{ count: number }>(
            `
    SELECT COUNT(*)::int AS count
    FROM outbox_events
    WHERE status IN ('pending', 'processing')
  `,
        );

        assert(
            pendingEvents.rows[0]!.count === 0,
            'Existing pending or processing outbox events detected. Aborting test.',
        );

        console.log('Creating five temporary jobs and outbox events...');

        const client = await pool.connect();

        try {
            await client.query('BEGIN');

            for (let index = 0; index < priorities.length; index++) {
                const jobId = testJobIds[index]!;
                const priority = priorities[index]!;

                await client.query(
                    `
        INSERT INTO jobs (
          job_id,
          job_type,
          payload,
          status,
          priority
        )
        VALUES ($1, $2, $3::jsonb, 'pending', $4)
      `,
                    [
                        jobId,
                        'priority_pipeline_test',
                        JSON.stringify({ test: true }),
                        priority,
                    ],
                );

                await client.query(
                    `
        INSERT INTO outbox_events (
          aggregate_type,
          aggregate_id,
          event_type,
          payload
        )
        VALUES ($1, $2, $3, $4::jsonb)
      `,
                    [
                        'job',
                        jobId,
                        'job.created',
                        JSON.stringify({ jobId }),
                    ],
                );
            }

            await client.query('COMMIT');
            insertedJobIds = [...testJobIds];
        } catch (error) {
            await client.query('ROLLBACK');
            throw error;
        } finally {
            client.release();
        }

        console.log('Five temporary jobs and outbox events created.');
        console.log('Original priorities:', priorities);

        console.log('Running the actual dispatcher service...');

        const publishedCount = await publishPendingOutboxEvents(
            publisherChannel,
            dispatcherId,
            priorities.length,
        );

        console.log('Published events:', publishedCount);

        assert(
            publishedCount === priorities.length,
            `Expected ${priorities.length} published events, received ${publishedCount}.`,
        );

        const publishedResult = await pool.query<{
            status: string;
            count: string;
        }>(
            `
        SELECT status, COUNT(*)::text AS count
        FROM outbox_events
        WHERE aggregate_type = 'job'
          AND aggregate_id = ANY($1::uuid[])
        GROUP BY status
      `,
            [testJobIds],
        );

        const publishedCountInDatabase = publishedResult.rows.find(
            (row) => row.status === 'published',
        )?.count;

        assert(
            publishedCountInDatabase === String(priorities.length),
            'Not all test outbox events were marked as published.',
        );

        console.log('All five outbox events marked as published.');

        const queueAfterPublish = await consumerChannel.checkQueue(TASK_QUEUE);

        assert(
            queueAfterPublish.messageCount === priorities.length,
            `Expected ${priorities.length} ready messages, found ${queueAfterPublish.messageCount}.`,
        );

        console.log('All five messages reached the production RabbitMQ queue.');

        await consumerChannel.prefetch(1);

        const receivedAllMessages = new Promise<void>((resolve, reject) => {
            const timeout = setTimeout(() => {
                reject(
                    new Error(
                        `Timed out. Received ${receivedPriorities.length}/${priorities.length} messages.`,
                    ),
                );
            }, 10000);

            consumerChannel!.consume(
                TASK_QUEUE,
                (message) => {
                    if (!message) {
                        return;
                    }

                    try {
                        const parsed = JSON.parse(message.content.toString()) as {
                            jobId?: string;
                            jobType?: string;
                            priority?: number;
                        };

                        assert(
                            typeof parsed.jobId === 'string' &&
                            testJobIdSet.has(parsed.jobId),
                            'Received a message that does not belong to this test.',
                        );

                        assert(
                            parsed.jobType === 'priority_pipeline_test',
                            'Unexpected job type received.',
                        );

                        assert(
                            Number.isSafeInteger(parsed.priority) &&
                            parsed.priority! >= 0 &&
                            parsed.priority! <= 10,
                            'Received message has an invalid priority.',
                        );

                        receivedJobIds.push(parsed.jobId);
                        receivedPriorities.push(parsed.priority!);

                        console.log(
                            `Received priority ${parsed.priority}, job ${parsed.jobId}`,
                        );

                        consumerChannel!.ack(message);

                        if (receivedPriorities.length === priorities.length) {
                            clearTimeout(timeout);
                            resolve();
                        }
                    } catch (error) {
                        // Leave the message unacknowledged.
                        // Cancelling the consumer will requeue it.
                        clearTimeout(timeout);
                        reject(error);
                    }
                },
                { noAck: false },
            )
                .then((result) => {
                    consumerTag = result.consumerTag;
                })
                .catch((error: unknown) => {
                    clearTimeout(timeout);
                    reject(error);
                });
        });

        await receivedAllMessages;

        console.log('Expected priority order:', expectedPriorities);
        console.log('Actual priority order:', receivedPriorities);

        assert(
            JSON.stringify(receivedPriorities) ===
            JSON.stringify(expectedPriorities),
            'RabbitMQ did not deliver messages in descending priority order.',
        );

        assert(
            new Set(receivedJobIds).size === priorities.length,
            'Duplicate or missing test job IDs detected.',
        );

        console.log('Dispatcher-to-RabbitMQ priority pipeline test PASSED.');
    } finally {
        if (consumerChannel && consumerTag) {
            try {
                await consumerChannel.cancel(consumerTag);
            } catch (error) {
                console.error('Consumer cancellation failed:', error);
            }
        }

        // Remove only messages belonging to this test.
        if (consumerChannel && insertedJobIds.length > 0) {
            try {
                for (let index = 0; index < priorities.length; index++) {
                    const message = await consumerChannel.get(TASK_QUEUE, {
                        noAck: false,
                    });

                    if (!message) {
                        break;
                    }

                    let belongsToTest = false;

                    try {
                        const parsed = JSON.parse(message.content.toString()) as {
                            jobId?: string;
                        };

                        belongsToTest =
                            typeof parsed.jobId === 'string' &&
                            testJobIdSet.has(parsed.jobId);
                    } catch {
                        belongsToTest = false;
                    }

                    if (belongsToTest) {
                        consumerChannel.ack(message);
                    } else {
                        consumerChannel.nack(message, false, true);
                        break;
                    }
                }
            } catch (error) {
                console.error('Test-message cleanup failed:', error);
            }
        }

        if (publisherChannel) {
            await publisherChannel.close().catch(console.error);
        }

        if (consumerChannel) {
            await consumerChannel.close().catch(console.error);
        }

        if (connection) {
            await connection.close().catch(console.error);
        }

        if (insertedJobIds.length > 0) {
            try {
                await pool.query(
                    `
            DELETE FROM outbox_events
            WHERE aggregate_type = 'job'
              AND aggregate_id = ANY($1::uuid[])
          `,
                    [insertedJobIds],
                );

                await pool.query(
                    `
            DELETE FROM jobs
            WHERE job_id = ANY($1::uuid[])
          `,
                    [insertedJobIds],
                );

                console.log('Temporary database records cleaned up.');
            } catch (error) {
                console.error('Database cleanup failed:', error);
            }
        }

        await pool.end();
    }
}

main().catch((error: unknown) => {
    console.error('Priority pipeline test FAILED:', error);
    process.exitCode = 1;
});
