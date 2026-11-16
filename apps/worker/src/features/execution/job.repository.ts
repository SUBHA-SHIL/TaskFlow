
import { withTransaction } from '@taskflow/database';

export interface ClaimedJob {
    jobId: string;
    jobType: string;
    payload: Record<string, unknown>;
    attempts: number;
    maxAttempts: number;
}

export async function claimJob(
    jobId: string,
    workerId: string,
): Promise<ClaimedJob | null> {
    return withTransaction(async (client) => {
        // Verify that the worker is registered and active.
        const workerResult = await client.query(
            `SELECT worker_id
             FROM workers
             WHERE worker_id = $1
               AND status = 'active'
             FOR SHARE`,
            [workerId],
        );

        if (workerResult.rowCount !== 1) {
            throw new Error(
                `Worker ${workerId} is not registered or active`,
            );
        }

        // Lock the job so another worker cannot claim it.
        const result = await client.query<{
            job_id: string;
            job_type: string;
            payload: Record<string, unknown>;
            status: string;
            attempts: number;
            max_attempts: number;
        }>(
            `SELECT
                job_id,
                job_type,
                payload,
                status,
                attempts,
                max_attempts
             FROM jobs
             WHERE job_id = $1
               AND status IN ('pending', 'queued', 'retry_scheduled')
               AND run_at <= NOW()
               AND cancel_requested_at IS NULL
               AND attempts < max_attempts
             FOR UPDATE SKIP LOCKED`,
            [jobId],
        );

        const job = result.rows[0];

        if (!job) {
            return null;
        }

        // Increment the current-cycle and lifetime counters.
        const updateResult = await client.query<{
            attempts: number;
            total_attempts: number;
        }>(
            `UPDATE jobs
             SET
                status = 'running',
                locked_by = $2,
                locked_at = NOW(),
                lease_expires_at = NOW() + INTERVAL '60 seconds',
                attempts = attempts + 1,
                total_attempts = total_attempts + 1,
                updated_at = NOW()
             WHERE job_id = $1
             RETURNING attempts, total_attempts`,
            [jobId, workerId],
        );

        const updatedJob = updateResult.rows[0];

        if (!updatedJob) {
            throw new Error(`Failed to claim job ${jobId}`);
        }

        // The lifetime number uniquely identifies the historical attempt.
        const attemptNumber = updatedJob.total_attempts;

        // Record this execution attempt.
        await client.query(
            `INSERT INTO job_attempts (
                job_id,
                worker_id,
                attempt_number,
                status
             )
             VALUES ($1, $2, $3, 'running')`,
            [jobId, workerId, attemptNumber],
        );

        // Record the lifecycle transition.
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
                'job.started',
                $3,
                'running',
                'worker',
                jsonb_build_object(
                    'attemptNumber', $4::integer,
                    'retryCycleAttempt', $5::integer
                )
             )`,
            [
                jobId,
                workerId,
                job.status,
                attemptNumber,
                updatedJob.attempts,
            ],
        );

        return {
            jobId: job.job_id,
            jobType: job.job_type,
            payload: job.payload,
            attempts: updatedJob.attempts,
            maxAttempts: job.max_attempts,
        };
    });
}
