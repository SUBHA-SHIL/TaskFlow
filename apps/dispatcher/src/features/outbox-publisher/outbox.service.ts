
import type { ConfirmChannel } from 'amqplib';

import { pool } from '@taskflow/database';
import { publishJob } from '@taskflow/messaging';

import {
  claimOutboxEvents,
  markOutboxEventAsFailed,
  markOutboxEventAsPublished,
} from './outbox.repository.js';

export async function publishPendingOutboxEvents(
  channel: ConfirmChannel,
  dispatcherId: string,
  batchSize = 10,
): Promise<number> {
  const events = await claimOutboxEvents(dispatcherId, batchSize);

  let publishedCount = 0;

  for (const event of events) {
    try {
      // Retrieve the authoritative job type from PostgreSQL.
      const result = await pool.query<{
        job_type: string;
        priority: number;
      }>(
        `
    SELECT job_type, priority
    FROM jobs
    WHERE job_id = $1
  `,
        [event.aggregateId],
      );

      const job = result.rows[0];

      if (!job) {
        throw new Error(`Job ${event.aggregateId} was not found`);
      }

      // Publish the job and wait for RabbitMQ confirmation.
      await publishJob(channel, {
        jobId: event.aggregateId,
        jobType: job.job_type,
        priority: job.priority,
      });

      // Mark the event published only after confirmation.
      await markOutboxEventAsPublished(
        event.outboxId,
        dispatcherId,
      );

      publishedCount++;

      console.log(
        `Outbox event ${event.outboxId} published successfully.`,
      );
    } catch (error) {
      console.error(
        `Failed to publish outbox event ${event.outboxId}:`,
        error,
      );

      try {
        await markOutboxEventAsFailed(
          event.outboxId,
          dispatcherId,
          event.attempts,
          error,
        );
      } catch (recordingError) {
        console.error(
          `Failed to record failure for outbox event ${event.outboxId}:`,
          recordingError,
        );
      }
    }
  }

  return publishedCount;
}
