import { z } from 'zod';

export const createJobSchema = z
    .object({
        jobType: z.string().trim().min(1).max(150),

        payload: z.record(z.string(), z.unknown()).default({}),

        priority: z.number().int().default(0),

        maxAttempts: z.number().int().min(1).max(100).default(3),

        idempotencyKey: z.string().trim().min(1).max(255).optional(),
    })
    .strict();


export const listJobsQuerySchema = z.object({
  page: z.coerce.number().int().min(1).default(1),

  limit: z.coerce.number().int().min(1).max(100).default(10),

  status: z.enum([
    'pending',
    'queued',
    'running',
    'completed',
    'failed',
    'retry_scheduled',
    'dead',
    'cancelled',
    'scheduled',
  ]).optional(),
});

export type ListJobsQuery = z.infer<typeof listJobsQuerySchema>;


export const jobIdSchema = z.uuid();
export type CreateJobInput = z.infer<typeof createJobSchema>;
