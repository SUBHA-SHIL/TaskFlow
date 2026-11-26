import { randomUUID } from 'node:crypto';
import { pool } from '@taskflow/database';

import { claimOutboxEvents } from '../src/features/outbox-publisher/outbox.repository.js';

const priorities = [1, 3, 5, 7, 10];
const testJobIds: string[] = [];

async function main(): Promise<void> {
  const dispatcherId = `priority-test-${randomUUID()}`;

  try {
    console.log('Creating temporary priority test jobs...');

    for (const priority of priorities) {
      const jobId = randomUUID();
      testJobIds.push(jobId);

      await pool.query(
        `
        INSERT INTO jobs (
          job_id,
          job_type,
          payload,
          priority
        )
        VALUES ($1, $2, $3, $4)
        `,
        [
          jobId,
          'priority_test',
          JSON.stringify({ test: true }),
          priority,
        ],
      );

      await pool.query(
        `
        INSERT INTO outbox_events (
          aggregate_type,
          aggregate_id,
          event_type,
          payload
        )
        VALUES ($1, $2, $3, $4)
        `,
        [
          'job',
          jobId,
          'job.created',
          JSON.stringify({ jobId }),
        ],
      );
    }

    console.log('Five temporary jobs created.');

    const claimedEvents = await claimOutboxEvents(
      dispatcherId,
      3,
    );

    const claimedIds = claimedEvents.map(
      (event) => event.aggregateId,
    );

    const result = await pool.query<{
      job_id: string;
      priority: number;
    }>(
      `
      SELECT job_id, priority
      FROM jobs
      WHERE job_id = ANY($1::uuid[])
      `,
      [claimedIds],
    );

    const actualPriorities = result.rows
      .map((row) => row.priority)
      .sort((a, b) => b - a);

    const expectedPriorities = [10, 7, 5];

    console.log('Expected selected priorities:', expectedPriorities);
    console.log('Actual selected priorities:', actualPriorities);

    if (
      claimedEvents.length !== 3 ||
      JSON.stringify(actualPriorities) !==
        JSON.stringify(expectedPriorities)
    ) {
      throw new Error(
        'Database priority selection test failed',
      );
    }

    console.log(
      'Database priority selection test PASSED.',
    );
  } finally {
    console.log('Cleaning up temporary test records...');

    await pool.query(
      `
      DELETE FROM outbox_events
      WHERE aggregate_type = 'job'
        AND aggregate_id = ANY($1::uuid[])
      `,
      [testJobIds],
    );

    await pool.query(
      `
      DELETE FROM jobs
      WHERE job_id = ANY($1::uuid[])
      `,
      [testJobIds],
    );

    await pool.end();

    console.log('Temporary test records cleaned up.');
  }
}

main().catch((error: unknown) => {
  console.error('Database priority test failed:', error);
  process.exitCode = 1;
});