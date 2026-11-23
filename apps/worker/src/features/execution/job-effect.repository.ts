
import {
  withTransaction,
  type PoolClient,
} from '@taskflow/database';

export interface JobEffectResult<T> {
  executed: boolean;
  result: T;
}

export async function executeIdempotentEffect<T>(
  jobId: string,
  effectKey: string,
  operation: (client: PoolClient) => Promise<T>,
): Promise<JobEffectResult<T>> {
  return withTransaction(async (client) => {
    const insertResult = await client.query(
      `INSERT INTO job_effects (
          job_id,
          effect_key,
          status
      )
      VALUES ($1, $2, 'completed')
      ON CONFLICT (job_id, effect_key)
      DO NOTHING
      RETURNING effect_id`,
      [jobId, effectKey],
    );

    if (insertResult.rows.length === 0) {
      const existingResult = await client.query(
        `SELECT result
         FROM job_effects
         WHERE job_id = $1
           AND effect_key = $2`,
        [jobId, effectKey],
      );

      if (existingResult.rows.length === 0) {
        throw new Error(
          `Unable to retrieve existing effect: ${effectKey}`,
        );
      }

      return {
        executed: false,
        result: existingResult.rows[0].result as T,
      };
    }

    // Execute the database operation in the same transaction.
    const result = await operation(client);

    const serializedResult = JSON.stringify(result);

    if (serializedResult === undefined) {
      throw new Error(
        `Effect result is not JSON-serializable: ${effectKey}`,
      );
    }

    await client.query(
      `UPDATE job_effects
       SET result = $1::jsonb
       WHERE effect_id = $2`,
      [
        serializedResult,
        insertResult.rows[0].effect_id,
      ],
    );

    return {
      executed: true,
      result,
    };
  });
}
