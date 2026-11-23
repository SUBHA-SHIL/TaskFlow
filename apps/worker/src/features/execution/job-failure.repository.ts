
import { withTransaction } from '@taskflow/database';

export interface JobFailureResult {
    status: 'retry_scheduled' | 'dead' | 'cancelled';
    attempts: number;
    retryAt: Date | null;
}

export async function failJob(
    jobId: string,
    workerId: string,
    error: unknown,
): Promise<JobFailureResult | null> {
    return withTransaction(async (client) => {
        // Verify that this worker still owns the running job.
        const jobResult = await client.query<{
            attempts: number;
            total_attempts: number;
            max_attempts: number;
            cancel_requested_at: Date | null;
        }>(
            `SELECT attempts, total_attempts, max_attempts, cancel_requested_at
             FROM jobs
             WHERE job_id = $1
               AND status = 'running'
               AND locked_by = $2
               AND lease_expires_at > clock_timestamp()
             FOR UPDATE`,
            [jobId, workerId],
        );

        const job = jobResult.rows[0];

        if (!job) {
            return null;
        }
        // Cancellation takes precedence over normal failure handling.
        if (job.cancel_requested_at !== null) {
            const attemptResult = await client.query(
                `UPDATE job_attempts
         SET
           status = 'cancelled',
           error_message = 'Job cancellation requested',
           finished_at = clock_timestamp()
         WHERE job_id = $1
           AND worker_id = $2
           AND attempt_number = $3
           AND status = 'running'`,
                [jobId, workerId, job.total_attempts],
            );

            if (attemptResult.rowCount !== 1) {
                throw new Error(
                    `Could not cancel attempt ${job.total_attempts} for job ${jobId}`,
                );
            }

            const updateResult = await client.query(
                `UPDATE jobs
         SET
           status = 'cancelled',
           completed_at = clock_timestamp(),
           locked_by = NULL,
           locked_at = NULL,
           lease_expires_at = NULL,
           updated_at = clock_timestamp()
         WHERE job_id = $1
           AND status = 'running'
           AND locked_by = $2
           AND cancel_requested_at IS NOT NULL
         RETURNING job_id`,
                [jobId, workerId],
            );

            if (updateResult.rowCount !== 1) {
                throw new Error(`Failed to cancel job ${jobId}`);
            }

            await client.query(
                `INSERT INTO job_events (
           job_id,
           worker_id,
           event_type,
           previous_status,
           new_status,
           actor_type,
           metadata
         )
         VALUES (
           $1,
           $2,
           'job.cancelled',
           'running',
           'cancelled',
           'worker',
           jsonb_build_object(
             'attemptNumber', $3::integer,
             'retryCycleAttempt', $4::integer
           )
         )`,
                [jobId, workerId, job.total_attempts, job.attempts],
            );

            return {
                status: 'cancelled',
                attempts: job.attempts,
                retryAt: null,
            };
        }

        const errorMessage =
            error instanceof Error ? error.message : String(error);

        // Retry eligibility is based on the current retry cycle.
        const isRetryable = job.attempts < job.max_attempts;

        const nextStatus = isRetryable
            ? 'retry_scheduled'
            : 'dead';

        // Complete the current historical attempt as failed.
        const attemptResult = await client.query(
            `UPDATE job_attempts
             SET
                status = 'failed',
                error_message = $4,
                finished_at = NOW()
             WHERE job_id = $1
               AND worker_id = $2
               AND attempt_number = $3
               AND status = 'running'`,
            [
                jobId,
                workerId,
                job.total_attempts,
                errorMessage.slice(0, 5000),
            ],
        );

        if (attemptResult.rowCount !== 1) {
            throw new Error(
                `Could not record failed attempt for job ${jobId}`,
            );
        }

        // Exponential backoff: 1, 2, 4, 8... capped at 300 seconds.
        const retryDelaySeconds = isRetryable
            ? Math.min(
                2 ** Math.min(job.attempts - 1, 9),
                300,
            )
            : null;

        // Update the authoritative job record.
        const updateResult = await client.query<{
            run_at: Date;
        }>(
            `UPDATE jobs
             SET
                status = $3::varchar,
                last_error = $4,
                run_at = CASE
                    WHEN $5::integer IS NOT NULL
                        THEN NOW() + ($5::integer * INTERVAL '1 second')
                    ELSE run_at
                END,
                completed_at = CASE
                    WHEN $6::boolean THEN NOW()
                    ELSE NULL
                END,
                locked_by = NULL,
                locked_at = NULL,
                lease_expires_at = NULL,
                updated_at = NOW()
             WHERE job_id = $1
               AND status = 'running'
               AND locked_by = $2
               AND lease_expires_at > clock_timestamp()
             RETURNING run_at`,
            [
                jobId,
                workerId,
                nextStatus,
                errorMessage.slice(0, 5000),
                retryDelaySeconds,
                !isRetryable,
            ],
        );

        const updatedJob = updateResult.rows[0];

        if (!updatedJob) {
            throw new Error(
                `Failed to update job ${jobId} after failure`,
            );
        }

        // Record the lifecycle event.
        await client.query(
            `INSERT INTO job_events (
                job_id,
                worker_id,
                event_type,
                previous_status,
                new_status,
                actor_type,
                metadata
             )
             VALUES (
                $1,
                $2,
                $3,
                'running',
                $4,
                'worker',
                jsonb_build_object(
                    'attemptNumber', $5::integer,
                    'retryCycleAttempt', $6::integer,
                    'error', $7::text,
                    'retryDelaySeconds', $8::integer
                )
             )`,
            [
                jobId,
                workerId,
                isRetryable ? 'job.retry_scheduled' : 'job.dead',
                nextStatus,
                job.total_attempts,
                job.attempts,
                errorMessage.slice(0, 5000),
                retryDelaySeconds,
            ],
        );

        // Schedule reliable publication at the retry time.
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
                [jobId, updatedJob.run_at],
            );
        }

        return {
            status: nextStatus,
            attempts: job.attempts,
            retryAt: isRetryable ? updatedJob.run_at : null,
        };
    });
}
