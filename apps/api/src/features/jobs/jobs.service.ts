import { randomUUID } from 'node:crypto';

import { withTransaction, pool } from '@taskflow/database';

import type { CreateJobInput, ListJobsQuery } from './jobs.schema.js';

import type {
    CreateJobResult,
    GetJobResponse,
    ListJobsResponse,
    JobListItem,
    CancelJobResponse,
    RetryJobResponse,
} from './jobs.types.js';

export class IdempotencyConflictError extends Error {
    constructor() {
        super('The supplied idempotency key has already been used with a different request');
        this.name = 'IdempotencyConflictError';
    }
}


export async function createJob(input: CreateJobInput): Promise<CreateJobResult> {
    const jobId = randomUUID();
    const eventId = randomUUID();

    return withTransaction(async (client) => {
        const insertResult = await client.query(
            `INSERT INTO jobs (
        job_id,
    job_type,
    payload,
    priority,
    max_attempts,
    idempotency_key,
    run_at,
    requested_run_at,
    status
      )
      VALUES (
    $1, $2, $3, $4, $5, $6,
    COALESCE($7::timestamptz, NOW()),
    $7::timestamptz,
    'pending'
)
      ON CONFLICT (idempotency_key)
      WHERE idempotency_key IS NOT NULL
      DO NOTHING
      RETURNING job_id, job_type, status, run_at, created_at`,
            [
                jobId,
                input.jobType,
                JSON.stringify(input.payload),
                input.priority,
                input.maxAttempts,
                input.idempotencyKey ?? null,
                input.runAt ?? null,
            ],
        );

        const insertedJob = insertResult.rows[0];

        if (!insertedJob) {
            const existingResult = await client.query(
                `SELECT
          job_id,
          job_type,
          status,
          run_at,
          created_at,
          (
            job_type IS DISTINCT FROM $2::text
            OR payload IS DISTINCT FROM $3::jsonb
            OR priority IS DISTINCT FROM $4::integer
            OR max_attempts IS DISTINCT FROM $5::integer
            OR requested_run_at IS DISTINCT FROM $6::timestamptz
          ) AS request_mismatch
        FROM jobs
        WHERE idempotency_key = $1`,
                [
                    input.idempotencyKey,
                    input.jobType,
                    JSON.stringify(input.payload),
                    input.priority,
                    input.maxAttempts,
                    input.runAt ?? null,
                ],
            );

            const existingJob = existingResult.rows[0];

            if (!existingJob) {
                throw new Error('Unable to retrieve the job for the supplied idempotency key');
            }

            if (existingJob.request_mismatch) {
                throw new IdempotencyConflictError();
            }

            return {
                job: {
                    jobId: existingJob.job_id,
                    status: existingJob.status,
                    jobType: existingJob.job_type,
                    runAt: existingJob.run_at.toISOString(),
                    createdAt: existingJob.created_at.toISOString(),
                },
                created: false,
            };
        }

        await client.query(
            `INSERT INTO outbox_events (
        outbox_id,
        aggregate_type,
        aggregate_id,
        event_type,
        payload,
        available_at
      )
      VALUES ($1, 'job', $2, 'job.created', $3, $4)`,
            [
                eventId,
                jobId,
                JSON.stringify({
                    eventId,
                    jobId,
                    jobType: input.jobType,
                }),
                insertedJob.run_at,
            ],
        );

        return {
            job: {
                jobId: insertedJob.job_id,
                status: insertedJob.status,
                jobType: insertedJob.job_type,
                runAt: insertedJob.run_at.toISOString(),
                createdAt: insertedJob.created_at.toISOString(),
            },
            created: true,
        };
    });
}


export async function getJobById(jobId: string): Promise<GetJobResponse | null> {
    const result = await pool.query(
        `SELECT
      job_id,
      job_type,
      status,
      payload,
      priority,
      attempts,
      total_attempts,
      max_attempts,
      run_at,
      result,
      last_error,
      created_at,
      updated_at,
      completed_at
    FROM jobs
    WHERE job_id = $1`,
        [jobId]
    );

    const job = result.rows[0];

    if (!job) {
        return null;
    }

    return {
        jobId: job.job_id,
        jobType: job.job_type,
        status: job.status,
        payload: job.payload,
        priority: job.priority,
        attempts: job.attempts,
        totalAttempts: job.total_attempts,
        maxAttempts: job.max_attempts,
        runAt: job.run_at.toISOString(),
        result: job.result,
        lastError: job.last_error,
        createdAt: job.created_at.toISOString(),
        updatedAt: job.updated_at.toISOString(),
        completedAt: job.completed_at?.toISOString() ?? null,
    };
}

export async function listJobs(query: ListJobsQuery): Promise<ListJobsResponse> {
    const { page, limit, status } = query;

    const offset = (page - 1) * limit;

    const values: unknown[] = [];

    let whereClause = '';

    if (status) {
        values.push(status);
        whereClause = `WHERE status = $${values.length}`;
    }

    const countResult = await pool.query<{ total: string }>(
        `SELECT COUNT(*) AS total
     FROM jobs
     ${whereClause}`,
        values
    );

    const totalItems = Number(countResult.rows[0]?.total ?? 0);

    const totalPages = Math.ceil(totalItems / limit);

    const paginationValues = [...values, limit, offset];

    const jobsResult = await pool.query(
        `SELECT
      job_id,
      job_type,
      status,
      priority,
      attempts,
      max_attempts,
      run_at,
      created_at,
      updated_at
     FROM jobs
     ${whereClause}
     ORDER BY created_at DESC, job_id DESC
     LIMIT $${values.length + 1}
     OFFSET $${values.length + 2}`,
        paginationValues
    );

    const jobs: JobListItem[] = jobsResult.rows.map((job) => ({
        jobId: job.job_id,
        jobType: job.job_type,
        status: job.status,
        priority: job.priority,
        attempts: job.attempts,
        maxAttempts: job.max_attempts,
        runAt: job.run_at.toISOString(),
        createdAt: job.created_at.toISOString(),
        updatedAt: job.updated_at.toISOString(),
    }));

    return {
        jobs,
        pagination: {
            page,
            limit,
            totalItems,
            totalPages,
            hasNextPage: page < totalPages,
            hasPreviousPage: page > 1 && totalPages > 0,
        },
    };
}

export async function cancelJob(jobId: string): Promise<CancelJobResponse | null> {
    return withTransaction(async (client) => {
        const result = await client.query(
            `SELECT job_id, status, cancel_requested_at
       FROM jobs
       WHERE job_id = $1
       FOR UPDATE`,
            [jobId]
        );

        const job = result.rows[0];

        if (!job) {
            return null;
        }

        const currentStatus = job.status;

        if (currentStatus === 'cancelled') {
            return {
                jobId,
                status: currentStatus,
                cancellationStatus: 'already_cancelled',
            };
        }

        const cancellableStatuses = ['pending', 'queued', 'scheduled', 'retry_scheduled'];

        if (cancellableStatuses.includes(currentStatus)) {
            await client.query(
                `UPDATE jobs
         SET status = 'cancelled',
             cancel_requested_at = NOW(),
             completed_at = NOW(),
             updated_at = NOW()
         WHERE job_id = $1`,
                [jobId]
            );

            await client.query(
                `INSERT INTO job_events (
          job_id,
          event_type,
          previous_status,
          new_status,
          actor_type
        )
        VALUES ($1, 'job.cancelled', $2, 'cancelled', 'api')`,
                [jobId, currentStatus]
            );

            return {
                jobId,
                status: 'cancelled',
                cancellationStatus: 'cancelled',
            };
        }

        if (currentStatus === 'running') {
            if (!job.cancel_requested_at) {
                await client.query(
                    `UPDATE jobs
           SET cancel_requested_at = NOW(),
               updated_at = NOW()
           WHERE job_id = $1`,
                    [jobId]
                );

                await client.query(
                    `INSERT INTO job_events (
            job_id,
            event_type,
            previous_status,
            new_status,
            actor_type
          )
          VALUES (
            $1,
            'job.cancellation_requested',
            'running',
            'running',
            'api'
          )`,
                    [jobId]
                );
            }

            return {
                jobId,
                status: 'running',
                cancellationStatus: 'cancellation_requested',
            };
        }

        return {
            jobId,
            status: currentStatus,
            cancellationStatus: 'not_cancellable',
        };
    });
}

export async function retryJob(jobId: string): Promise<RetryJobResponse | null> {
    return withTransaction(async (client) => {
        const result = await client.query(
            `SELECT job_id, status
       FROM jobs
       WHERE job_id = $1
       FOR UPDATE`,
            [jobId]
        );

        const job = result.rows[0];

        if (!job) {
            return null;
        }

        const previousStatus = job.status;

        if (previousStatus !== 'failed' && previousStatus !== 'dead') {
            throw new Error(`Job cannot be retried because its current status is '${previousStatus}'`);
        }

        await client.query(
            `UPDATE jobs
       SET status = 'pending',
           attempts = 0,
           run_at = NOW(),
           locked_by = NULL,
           locked_at = NULL,
           lease_expires_at = NULL,
           completed_at = NULL,
           last_error = NULL,
           result = NULL,
           cancel_requested_at = NULL,
           updated_at = NOW()
       WHERE job_id = $1`,
            [jobId]
        );

        await client.query(
            `INSERT INTO job_events (
        job_id,
        event_type,
        previous_status,
        new_status,
        actor_type
      )
      VALUES ($1, 'job.retry_requested', $2, 'pending', 'api')`,
            [jobId, previousStatus]
        );

        await client.query(
            `INSERT INTO outbox_events (
    aggregate_type,
    aggregate_id,
    event_type,
    payload
  )
  VALUES (
    'job',
    $1,
    'job.retry_requested',
    jsonb_build_object('jobId', $2::text)
  )`,
            [jobId, jobId]
        );

        return {
            jobId,
            status: 'pending',
            previousStatus,
            attempts: 0,
        };
    });
}
