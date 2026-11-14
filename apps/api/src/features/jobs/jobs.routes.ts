import type { FastifyPluginAsync } from 'fastify';


import { createJobSchema, jobIdSchema, listJobsQuerySchema } from './jobs.schema.js';

import { createJob, getJobById, listJobs, cancelJob, retryJob } from './jobs.service.js';

export const jobsRoutes: FastifyPluginAsync = async (app) => {
    app.post('/jobs', async (request, reply) => {
        const validationResult = createJobSchema.safeParse(request.body);

        if (!validationResult.success) {
            return reply.status(400).send({
                error: {
                    code: 'VALIDATION_ERROR',
                    message: 'Invalid job request',
                    details: validationResult.error.issues,
                },
            });
        }

        const job = await createJob(validationResult.data);

        return reply.status(202).send({
            message: 'Job accepted successfully',
            data: job,
        });
    });

    app.get('/jobs', async (request, reply) => {
        const validationResult = listJobsQuerySchema.safeParse(
            request.query,
        );

        if (!validationResult.success) {
            return reply.status(400).send({
                error: {
                    code: 'VALIDATION_ERROR',
                    message: 'Invalid job listing parameters',
                    details: validationResult.error.issues,
                },
            });
        }

        const result = await listJobs(validationResult.data);

        return reply.status(200).send({
            data: result.jobs,
            pagination: result.pagination,
        });
    });

    app.get<{ Params: { jobId: string } }>(
        '/jobs/:jobId',
        async (request, reply) => {
            const validationResult = jobIdSchema.safeParse(
                request.params.jobId,
            );

            if (!validationResult.success) {
                return reply.status(400).send({
                    error: {
                        code: 'INVALID_JOB_ID',
                        message: 'The provided job ID must be a valid UUID',
                    },
                });
            }

            const job = await getJobById(validationResult.data);

            if (!job) {
                return reply.status(404).send({
                    error: {
                        code: 'JOB_NOT_FOUND',
                        message: 'Job not found',
                    },
                });
            }

            return reply.status(200).send({
                data: job,
            });
        },
    );

    app.delete<{ Params: { jobId: string } }>(
        '/jobs/:jobId',
        async (request, reply) => {
            const validation = jobIdSchema.safeParse(request.params.jobId);

            if (!validation.success) {
                return reply.code(400).send({
                    error: {
                        code: 'INVALID_JOB_ID',
                        message: 'The provided job ID is invalid.',
                    },
                });
            }

            const result = await cancelJob(validation.data);

            if (!result) {
                return reply.code(404).send({
                    error: {
                        code: 'JOB_NOT_FOUND',
                        message: 'The requested job was not found.',
                    },
                });
            }

            if (result.cancellationStatus === 'not_cancellable') {
                return reply.code(409).send({
                    error: {
                        code: 'JOB_NOT_CANCELLABLE',
                        message: 'The job cannot be cancelled in its current state.',
                    },
                    data: result,
                });
            }

            return reply.code(200).send({
                data: result,
            });
        },
    );

    app.post<{ Params: { jobId: string } }>(
        '/jobs/:jobId/retry',
        async (request, reply) => {
            const validation = jobIdSchema.safeParse(request.params.jobId);

            if (!validation.success) {
                return reply.code(400).send({
                    error: {
                        code: 'INVALID_JOB_ID',
                        message: 'Invalid job ID format',
                    },
                });
            }

            try {
                const result = await retryJob(validation.data);

                if (!result) {
                    return reply.code(404).send({
                        error: {
                            code: 'JOB_NOT_FOUND',
                            message: 'Job not found',
                        },
                    });
                }

                return reply.code(200).send({
                    data: result,
                });
            } catch (error) {
                if (error instanceof Error && error.message.includes('cannot be retried')) {
                    return reply.code(409).send({
                        error: {
                            code: 'JOB_NOT_RETRYABLE',
                            message: error.message,
                        },
                    });
                }

                throw error;
            }
        },
    );

};
