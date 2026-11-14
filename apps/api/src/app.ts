import Fastify from 'fastify';

import { env } from './config/env.js';
import { healthRoutes } from './features/health/health.routes.js';
import { registerErrorHandler } from './shared/errors/error-handler.js';
import { jobsRoutes } from './features/jobs/jobs.routes.js';

export function buildApp() {
  const app = Fastify({
    logger: {
      level: env.NODE_ENV === 'development' ? 'debug' : 'info',
    },
  });

  registerErrorHandler(app);

  app.register(healthRoutes);
  app.register(jobsRoutes);

  return app;
}