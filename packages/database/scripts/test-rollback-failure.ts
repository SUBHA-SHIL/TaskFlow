import assert from 'node:assert/strict';
import type { PoolClient } from 'pg';

import { pool } from '../src/client.js';
import { withTransaction } from '../src/transaction.js';

async function main(): Promise<void> {
  const originalConnect = pool.connect.bind(pool);

  const transactionError = new Error('Simulated transaction failure');
  const rollbackError = new Error('Simulated rollback failure');

  let releasedWithError: Error | undefined;

  const fakeClient = {
    query: async (sql: string) => {
      if (sql === 'ROLLBACK') {
        throw rollbackError;
      }

      return { rows: [], rowCount: 0 };
    },

    release: (error?: Error) => {
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
          throw transactionError;
        }),
      (error: unknown) => error === transactionError
    );

    assert.equal(
      releasedWithError,
      rollbackError,
      'Failed connection should be released with the rollback error'
    );

    console.log('PASS: transaction failure was propagated.');
    console.log('PASS: rollback failure was detected.');
    console.log('PASS: broken connection was released with an error.');
    console.log('PASS: original transaction error was preserved.');
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
  console.error('Rollback failure test failed:', error);
  process.exitCode = 1;
});
