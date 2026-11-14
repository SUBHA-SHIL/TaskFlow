import type { FastifyInstance } from 'fastify';

type HttpError = Error & {
  statusCode?: number;
};

function isHttpError(error: unknown): error is HttpError {
  return error instanceof Error;
}

export function registerErrorHandler(app: FastifyInstance): void {
  app.setErrorHandler((error, request, reply) => {
    request.log.error({ err: error }, 'Request failed');

    const statusCode =
      isHttpError(error) &&
      typeof error.statusCode === 'number' &&
      error.statusCode >= 400 &&
      error.statusCode <= 599
        ? error.statusCode
        : 500;

    const message =
      statusCode >= 500
        ? 'An unexpected error occurred'
        : isHttpError(error)
          ? error.message
          : 'Request failed';

    reply.status(statusCode).send({
      error: {
        code:
          statusCode >= 500
            ? 'INTERNAL_SERVER_ERROR'
            : 'REQUEST_ERROR',
        message,
      },
    });
  });
}
