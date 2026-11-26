
import { withTransaction } from '@taskflow/database';

const LEASE_EXPIRED_ERROR =
    'Worker lease expired before job completion';

const CANCELLATION_ERROR =
    'Job cancellation requested before worker lease expired';

const MAX_RETRY_DELAY_SECONDS = 300;

interface ExpiredJob {
    job_id: string;
    attempts: number;
    total_attempts: number;
    max_attempts: number;
    locked_by: string | null;
    lease_expires_at: Date;
    cancellation_requested: boolean;
}

export async function recoverExpiredJobs(
    batchSize = 10,
): Promise<number> {
    if (!Number.isSafeInteger(batchSize) || batchSize < 1) {
        throw new Error('Recovery batch size must be a positive integer');
    }

    return withTransaction(async (client) => {
        const expiredJobsResult = await client.query<ExpiredJob>(
            `SELECT
                job_id,
                attempts,
                total_attempts,
                max_attempts,
                locked_by,
                lease_expires_at,
                cancel_requested_at IS NOT NULL AS cancellation_requested
             FROM jobs
             WHERE status = 'running'
               AND lease_expires_at <= clock_timestamp()
             ORDER BY lease_expires_at ASC
             LIMIT $1::integer
             FOR UPDATE SKIP LOCKED`,
            [batchSize],
        );

        const expiredJobs = expiredJobsResult.rows;

        for (const job of expiredJobs) {
            const cancellationRequested = job.cancellation_requested;

            // Cancellation takes precedence over retry eligibility.
            const isRetryable =
                !cancellationRequested &&
                job.attempts < job.max_attempts;

            const nextStatus = cancellationRequested
                ? 'cancelled'
                : isRetryable
                    ? 'retry_scheduled'
                    : 'dead';

            const attemptError = cancellationRequested
                ? CANCELLATION_ERROR
                : LEASE_EXPIRED_ERROR;

            // Attempt history uses the lifetime attempt number.
            const attemptResult = await client.query(
                `UPDATE job_attempts
                 SET
                    status = $3::varchar,
                    error_message = $4,
                    finished_at = clock_timestamp()
                 WHERE job_id = $1
                   AND attempt_number = $2
                   AND status = 'running'`,
                [
                    job.job_id,
                    job.total_attempts,
                    cancellationRequested ? 'cancelled' : 'abandoned',
                    attemptError,
                ],
            );

            if (attemptResult.rowCount !== 1) {
                throw new Error(
                    `Could not finalize expired attempt ${job.total_attempts} ` +
                    `for job ${job.job_id}`,
                );
            }

            // Backoff is based on the current retry-cycle attempt.
            const retryDelaySeconds = isRetryable
                ? Math.min(
                    2 ** Math.min(job.attempts - 1, 9),
                    MAX_RETRY_DELAY_SECONDS,
                )
                : null;

            const updateResult = await client.query<{
                run_at: Date;
            }>(
                `UPDATE jobs
                 SET
                    status = $2::varchar,
                    last_error = CASE
                        WHEN $6::boolean THEN NULL
                        ELSE $3
                    END,
                    run_at = CASE
                        WHEN $4::integer IS NOT NULL
                            THEN clock_timestamp()
                                + ($4::integer * INTERVAL '1 second')
                        ELSE run_at
                    END,
                    completed_at = CASE
                        WHEN $5::boolean THEN clock_timestamp()
                        ELSE NULL
                    END,
                    locked_by = NULL,
                    locked_at = NULL,
                    lease_expires_at = NULL,
                    updated_at = clock_timestamp()
                 WHERE job_id = $1
                   AND status = 'running'
                   AND lease_expires_at <= clock_timestamp()
                 RETURNING run_at`,
                [
                    job.job_id,
                    nextStatus,
                    LEASE_EXPIRED_ERROR,
                    retryDelaySeconds,
                    !isRetryable,
                    cancellationRequested,
                ],
            );

            const updatedJob = updateResult.rows[0];

            if (!updatedJob) {
                throw new Error(
                    `Failed to recover expired job ${job.job_id}`,
                );
            }

            await client.query(
                `INSERT INTO job_events (
                    job_id,
                    event_type,
                    previous_status,
                    new_status,
                    actor_type,
                    metadata
                 )
                 VALUES (
                    $1,
                    $2,
                    'running',
                    $3,
                    'system',
                    jsonb_build_object(
                        'attemptNumber', $4::integer,
                        'retryCycleAttempt', $5::integer,
                        'previousWorkerId', $6::text,
                        'leaseExpiredAt', $7::timestamptz,
                        'error', $8::text,
                        'cancellationRequested', $9::boolean
                    )
                 )`,
                [
                    job.job_id,
                    cancellationRequested
                        ? 'job.cancelled'
                        : 'job.lease_expired',
                    nextStatus,
                    job.total_attempts,
                    job.attempts,
                    job.locked_by,
                    job.lease_expires_at,
                    attemptError,
                    cancellationRequested,
                ],
            );

            // Only retryable jobs produce retry outbox events.
            if (isRetryable) {
                await client.query(
                    `INSERT INTO outbox_events (
                        aggregate_type,
                        aggregate_id,
                        event_type,
                        payload,
                        available_at
                     )
                     VALUES (
                        'job',
                        $1::uuid,
                        'job.retry_scheduled',
                        jsonb_build_object('jobId', $1::uuid::text),
                        $2
                     )`,
                    [job.job_id, updatedJob.run_at],
                );
            }
        }

        return expiredJobs.length;
    });
}
