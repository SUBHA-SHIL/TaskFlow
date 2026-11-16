
import { pool } from '@taskflow/database';

export const JOB_LEASE_DURATION_SECONDS = 60;

export async function renewJobLease(
  jobId: string,
  workerId: string,
): Promise<boolean> {
  const result = await pool.query(
    `UPDATE jobs
     SET
       lease_expires_at = clock_timestamp()
         + ($3::integer * INTERVAL '1 second'),
       updated_at = clock_timestamp()
     WHERE job_id = $1
       AND status = 'running'
       AND locked_by = $2
       AND lease_expires_at > clock_timestamp()
     RETURNING job_id`,
    [jobId, workerId, JOB_LEASE_DURATION_SECONDS],
  );

  return result.rowCount === 1;
}
