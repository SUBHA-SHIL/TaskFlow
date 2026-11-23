import { withTransaction } from '@taskflow/database';

export type JobCompletionResult =
  | 'completed'
  | 'cancellation_requested'
  | 'ownership_lost';

export async function completeJob(
  jobId: string,
  workerId: string,
  result: unknown,
): Promise<JobCompletionResult> {
  return withTransaction(async (client) => {
    // Verify ownership and retrieve the cancellation state.
    const jobResult = await client.query<{
      attempts: number;
      total_attempts: number;
      cancel_requested_at: Date | null;
    }>(
      `SELECT attempts, total_attempts, cancel_requested_at
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
      return 'ownership_lost';
    }

    // Cancellation takes precedence over successful completion.
    if (job.cancel_requested_at !== null) {
      return 'cancellation_requested';
    }

    const serializedResult = JSON.stringify(result ?? null);

    // Mark the job as completed.
    const updateResult = await client.query(
      `UPDATE jobs
       SET
         status = 'completed',
         result = $3::jsonb,
         completed_at = clock_timestamp(),
         locked_by = NULL,
         locked_at = NULL,
         lease_expires_at = NULL,
         updated_at = clock_timestamp()
       WHERE job_id = $1
         AND status = 'running'
         AND locked_by = $2
         AND lease_expires_at > clock_timestamp()
         AND cancel_requested_at IS NULL`,
      [jobId, workerId, serializedResult],
    );

    if (updateResult.rowCount !== 1) {
      throw new Error(
        `Worker ${workerId} lost ownership of job ${jobId}`,
      );
    }

    // Complete the corresponding lifetime execution attempt.
    const attemptResult = await client.query(
      `UPDATE job_attempts
       SET
         status = 'completed',
         result = $3::jsonb,
         finished_at = clock_timestamp()
       WHERE job_id = $1
         AND worker_id = $2
         AND attempt_number = $4
         AND status = 'running'`,
      [jobId, workerId, serializedResult, job.total_attempts],
    );

    if (attemptResult.rowCount !== 1) {
      throw new Error(
        `Could not complete attempt ${job.total_attempts} for job ${jobId}`,
      );
    }

    // Record the completed lifecycle transition.
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
         'job.completed',
         'running',
         'completed',
         'worker',
         jsonb_build_object(
           'attemptNumber', $3::integer,
           'retryCycleAttempt', $4::integer
         )
       )`,
      [jobId, workerId, job.total_attempts, job.attempts],
    );

    return 'completed';
  });
}
