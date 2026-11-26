import type { PoolClient } from 'pg';

import { pool } from './client.js';

export async function withTransaction<T>(callback: (client: PoolClient) => Promise<T>): Promise<T> {
  const client = await pool.connect();

  let transactionStarted = false;
  let releaseError: Error | undefined;

  try {
    await client.query('BEGIN');
    transactionStarted = true;

    const result = await callback(client);

    await client.query('COMMIT');
    transactionStarted = false;

    return result;
  } catch (error) {
    if (transactionStarted) {
      try {
        await client.query('ROLLBACK');
      } catch (rollbackError) {
        releaseError =
          rollbackError instanceof Error ? rollbackError : new Error(String(rollbackError));

        console.error('Transaction rollback failed:', rollbackError);
      }
    }

    throw error;
  } finally {
    client.release(releaseError);
  }
}
