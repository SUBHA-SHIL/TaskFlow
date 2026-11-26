import assert from 'node:assert/strict';
import type { PoolClient } from 'pg';

import { pool } from '../src/client.js';
import { withTransaction } from '../src/transaction.js';

async function main(): Promise<void> {
  const originalConnect = pool.connect.bind(pool);

  const commitError = new Error('Simulated COMMIT failure');

  const executedQueries: string[] = [];
  let clientReleased = false;
  let releasedWithError: Error | undefined;

  const fakeClient = {
    query: async (sql: string) => {
      executedQueries.push(sql);

      if (sql === 'COMMIT') {
        throw commitError;
      }

      return { rows: [], rowCount: 0 };
    },

    release: (error?: Error) => {
      clientReleased = true;
      releasedWithError = error;
    },
  } as unknown as PoolClient;

  try {
    (
      pool as unknown as {
        connect: () => Promise<PoolClient>;
      }
    ).connect = async () => fakeClient;

    await assert.rejects(
      () =>
        withTransaction(async () => {
          return 'transaction result';
        }),
      (error: unknown) => error === commitError
    );

    assert.deepEqual(
      executedQueries,
      ['BEGIN', 'COMMIT', 'ROLLBACK'],
      'Rollback should be attempted after COMMIT failure'
    );

    assert.equal(clientReleased, true, 'Client should be released');

    assert.equal(
      releasedWithError,
      undefined,
      'Successful rollback should release the client normally'
    );

    console.log('PASS: COMMIT failure was propagated.');
    console.log('PASS: ROLLBACK was attempted.');
    console.log('PASS: original COMMIT error was preserved.');
    console.log('PASS: client was released normally after successful rollback.');
  } finally {
    (
      pool as unknown as {
        connect: () => Promise<PoolClient>;
      }
    ).connect = originalConnect;

    await pool.end();
  }
}

main().catch((error: unknown) => {
  console.error('COMMIT failure test failed:', error);
  process.exitCode = 1;
});
