import os from 'node:os';

import { pool } from '@taskflow/database';

export interface RegisteredWorker {
  workerId: string;
  workerName: string;
}

export async function registerWorker(): Promise<RegisteredWorker> {
  const hostname = os.hostname();
  const workerName = `taskflow-worker-${hostname}`;

  const result = await pool.query<{
    worker_id: string;
    worker_name: string;
  }>(
    `INSERT INTO workers (
       worker_name,
       hostname,
       status,
       last_heartbeat_at
     )
     VALUES ($1, $2, 'active', NOW())
     RETURNING worker_id, worker_name`,
    [workerName, hostname],
  );

  const worker = result.rows[0];

  if (!worker) {
    throw new Error('Failed to register worker');
  }

  console.log('Worker registered:', {
    workerId: worker.worker_id,
    workerName: worker.worker_name,
    hostname,
  });

  return {
    workerId: worker.worker_id,
    workerName: worker.worker_name,
  };
}

export async function stopWorker(workerId: string): Promise<void> {
  await pool.query(
    `UPDATE workers
     SET
       status = 'stopped',
       updated_at = NOW()
     WHERE worker_id = $1`,
    [workerId],
  );

  console.log(`Worker ${workerId} marked as stopped.`);
}
