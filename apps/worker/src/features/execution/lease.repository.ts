
import { pool } from '@taskflow/database';

export const JOB_LEASE_DURATION_SECONDS = 60;

export type LeaseRenewalResult =
  | 'renewed'
  | 'cancellation_requested'
  | 'ownership_lost';

export async function renewJobLease(
  jobId: string,
  workerId: string,
): Promise<LeaseRenewalResult> {
  // Renew only if the worker still owns the job
  // and no cancellation has been requested.
  const renewalResult = await pool.query(
    `UPDATE jobs
     SET
       lease_expires_at = clock_timestamp()
         + ($3::integer * INTERVAL '1 second'),
       updated_at = clock_timestamp()
     WHERE job_id = $1
       AND status = 'running'
       AND locked_by = $2
       AND lease_expires_at > clock_timestamp()
       AND cancel_requested_at IS NULL
     RETURNING job_id`,
    [jobId, workerId, JOB_LEASE_DURATION_SECONDS],
  );

  if (renewalResult.rowCount === 1) {
    return 'renewed';
  }

  // Determine whether renewal failed because cancellation
  // was requested or because ownership was lost.
  const cancellationResult = await pool.query(
    `SELECT job_id
     FROM jobs
     WHERE job_id = $1
       AND status = 'running'
       AND locked_by = $2
       AND lease_expires_at > clock_timestamp()
       AND cancel_requested_at IS NOT NULL`,
    [jobId, workerId],
  );

  if (cancellationResult.rowCount === 1) {
    return 'cancellation_requested';
  }

  return 'ownership_lost';
}

export type CancellationCheckResult =
  | 'active'
  | 'cancellation_requested'
  | 'ownership_lost';

export async function checkJobCancellation(
  jobId: string,
  workerId: string,
): Promise<CancellationCheckResult> {
  const result = await pool.query<{
    cancel_requested_at: Date | null;
  }>(
    `SELECT cancel_requested_at
     FROM jobs
     WHERE job_id = $1
       AND status = 'running'
       AND locked_by = $2
       AND lease_expires_at > clock_timestamp()`,
    [jobId, workerId],
  );

  const job = result.rows[0];

  if (!job) {
    return 'ownership_lost';
  }

  if (job.cancel_requested_at !== null) {
    return 'cancellation_requested';
  }

  return 'active';
}