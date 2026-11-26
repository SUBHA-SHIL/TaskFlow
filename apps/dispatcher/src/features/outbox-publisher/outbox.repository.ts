import { withTransaction, pool } from '@taskflow/database';

import type { OutboxEvent } from './outbox.types.js';

interface OutboxEventRow {
    outbox_id: string;
    aggregate_id: string;
    event_type: string;
    payload: {
        jobId: string;
    };
    attempts: number;
}

const OUTBOX_LOCK_TIMEOUT_SECONDS = 300;

export async function claimOutboxEvents(
    dispatcherId: string,
    limit = 10,
): Promise<OutboxEvent[]> {
    return withTransaction(async (client) => {
        const result = await client.query<OutboxEventRow>(
            `
        WITH available_events AS (
    SELECT events.outbox_id
    FROM outbox_events AS events
    LEFT JOIN jobs
        ON events.aggregate_type = 'job'
        AND jobs.job_id = events.aggregate_id
    WHERE events.available_at <= NOW()
      AND (
        events.status = 'pending'
        OR (
          events.status = 'processing'
          AND events.locked_at < NOW() -
            ($2::int * INTERVAL '1 second')
        )
      )
    ORDER BY
        COALESCE(jobs.priority, 0) DESC,
        events.available_at ASC,
        events.created_at ASC
    LIMIT $1
    FOR UPDATE OF events SKIP LOCKED
)
UPDATE outbox_events AS events
SET
    status = 'processing',
    locked_at = NOW(),
    locked_by = $3,
    attempts = events.attempts + 1,
    updated_at = NOW()
FROM available_events
WHERE events.outbox_id = available_events.outbox_id
RETURNING
    events.outbox_id,
    events.aggregate_id,
    events.event_type,
    events.payload,
    events.attempts
      `,
            [limit, OUTBOX_LOCK_TIMEOUT_SECONDS, dispatcherId],
        );

        return result.rows.map((row) => ({
            outboxId: row.outbox_id,
            aggregateId: row.aggregate_id,
            eventType: row.event_type,
            payload: row.payload,
            attempts: row.attempts,
        }));
    });
}

export async function markOutboxEventAsPublished(
    outboxId: string,
    dispatcherId: string,
): Promise<void> {
    const result = await pool.query(
        `
      UPDATE outbox_events
      SET
        status = 'published',
        published_at = NOW(),
        locked_at = NULL,
        locked_by = NULL,
        last_error = NULL,
        updated_at = NOW()
      WHERE outbox_id = $1
        AND status = 'processing'
        AND locked_by = $2
    `,
        [outboxId, dispatcherId],
    );

    if (result.rowCount !== 1) {
        throw new Error(
            `Could not mark outbox event ${outboxId} as published: ` +
            'event is not owned by this dispatcher',
        );
    }
}

export async function markOutboxEventAsFailed(
    outboxId: string,
    dispatcherId: string,
    attempts: number,
    error: unknown,
): Promise<void> {
    const delaySeconds = Math.min(
        2 ** Math.min(Math.max(attempts - 1, 0), 9),
        300,
    );

    const errorMessage =
        error instanceof Error ? error.message : String(error);

    const result = await pool.query(
        `
      UPDATE outbox_events
      SET
        status = 'pending',
        available_at = NOW() + ($3::int * INTERVAL '1 second'),
        locked_at = NULL,
        locked_by = NULL,
        last_error = $4,
        updated_at = NOW()
      WHERE outbox_id = $1
        AND status = 'processing'
        AND locked_by = $2
    `,
        [
            outboxId,
            dispatcherId,
            delaySeconds,
            errorMessage.slice(0, 5000),
        ],
    );

    if (result.rowCount !== 1) {
        throw new Error(
            `Could not release outbox event ${outboxId}: ` +
            'event is not owned by this dispatcher',
        );
    }
}