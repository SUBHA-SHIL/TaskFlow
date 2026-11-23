import { withTransaction } from '@taskflow/database';

export async function cancelRunningJob(
  jobId: string,
  workerId: string,
): Promise<boolean> {
  return withTransaction(async (client) => {
    // Verify ownership, lease validity, and cancellation request.
    const jobResult = await client.query<{
      attempts: number;
      total_attempts: number;
    }>(
      `SELECT attempts, total_attempts
       FROM jobs
       WHERE job_id = $1
         AND status = 'running'
         AND locked_by = $2
         AND lease_expires_at > clock_timestamp()
         AND cancel_requested_at IS NOT NULL
       FOR UPDATE`,
      [jobId, workerId],
    );

    const job = jobResult.rows[0];

    if (!job) {
      return false;
    }

    // Close the current execution attempt as cancelled.
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

    // Persist the cancelled job state and release ownership.
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
         AND lease_expires_at > clock_timestamp()`,
      [jobId, workerId],
    );

    if (updateResult.rowCount !== 1) {
      throw new Error(
        `Failed to cancel job ${jobId} after cancelling its attempt`,
      );
    }

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

    return true;
  });
}
