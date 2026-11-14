import { buildApp } from './app.js';
import { env } from './config/env.js';

async function startServer(): Promise<void> {
  const app = buildApp();

  try {
    await app.listen({
      port: env.PORT,
      host: env.HOST,
    });

    app.log.info(
      `TaskFlow API running at http://${env.HOST}:${env.PORT}`,
    );
  } catch (error) {
    app.log.error(error, 'Failed to start TaskFlow API');
    process.exitCode = 1;
    await app.close();
    return;
  }

  const shutdown = async (signal: string): Promise<void> => {
    app.log.info({ signal }, 'Shutting down TaskFlow API');

    try {
      await app.close();
      app.log.info('TaskFlow API shut down successfully');
      process.exitCode = 0;
    } catch (error) {
      app.log.error(error, 'Error during server shutdown');
      process.exitCode = 1;
    }
  };

  process.once('SIGINT', () => void shutdown('SIGINT'));
  process.once('SIGTERM', () => void shutdown('SIGTERM'));
}

void startServer();