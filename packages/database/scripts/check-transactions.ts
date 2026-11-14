
import { randomUUID } from 'node:crypto';

import {
  closeDatabaseConnection,
  pool,
  withTransaction,
} from '../src/index.js';

async function main(): Promise<void> {
  const testId = randomUUID();
  const rollbackName = `transaction-rollback-${testId}`;
  const commitName = `transaction-commit-${testId}`;

  try {
    // Test 1: A failed transaction must roll back its insert.
    try {
      await withTransaction(async (client) => {
        await client.query(
          'INSERT INTO workers (worker_name) VALUES ($1)',
          [rollbackName],
        );

        throw new Error('Force rollback');
      });
    } catch (error) {
      if (!(error instanceof Error) || error.message !== 'Force rollback') {
        throw error;
      }
    }

    const rollbackResult = await pool.query(
      'SELECT COUNT(*)::int AS count FROM workers WHERE worker_name = $1',
      [rollbackName],
    );

    if (rollbackResult.rows[0].count !== 0) {
      throw new Error('Rollback test failed: worker record still exists');
    }

    console.log('Rollback test passed.');

    // Test 2: A successful transaction must commit its insert.
    await withTransaction(async (client) => {
      await client.query(
        'INSERT INTO workers (worker_name) VALUES ($1)',
        [commitName],
      );
    });

    const commitResult = await pool.query(
      'SELECT COUNT(*)::int AS count FROM workers WHERE worker_name = $1',
      [commitName],
    );

    if (commitResult.rows[0].count !== 1) {
      throw new Error('Commit test failed: worker record was not saved');
    }

    console.log('Commit test passed.');
    console.log('All transaction tests passed!');
  } finally {
    // Remove test records, even if a test fails.
    await pool.query(
      'DELETE FROM workers WHERE worker_name IN ($1, $2)',
      [rollbackName, commitName],
    );

    await closeDatabaseConnection();
  }
}

main().catch((error: unknown) => {
  console.error('Transaction tests failed:', error);
  process.exitCode = 1;
});
