import { pool } from '@taskflow/database';

const STALE_WORKER_THRESHOLD_SECONDS = 30;

export async function markStaleWorkersLost(): Promise<number> {
  const result = await pool.query(
    `UPDATE workers
     SET
       status = 'lost',
       updated_at = clock_timestamp()
     WHERE status = 'active'
       AND last_heartbeat_at IS NOT NULL
       AND last_heartbeat_at <
           clock_timestamp() - ($1 * INTERVAL '1 second')
     RETURNING worker_id, worker_name`,
    [STALE_WORKER_THRESHOLD_SECONDS],
  );

  if (result.rowCount && result.rowCount > 0) {
    console.warn('Stale workers marked as lost:', result.rows);
  }

  return result.rowCount ?? 0;
}