import type { JobHandler } from './handler.types.js';

const handlers = new Map<string, JobHandler>();

export function registerHandler(
  jobType: string,
  handler: JobHandler,
): void {
  const normalizedJobType = jobType.trim();

  if (!normalizedJobType) {
    throw new Error('Job type cannot be empty');
  }

  if (handlers.has(normalizedJobType)) {
    throw new Error(
      `Handler already registered for job type: ${normalizedJobType}`,
    );
  }

  handlers.set(normalizedJobType, handler);
}

export function getHandler(
  jobType: string,
): JobHandler | undefined {
  return handlers.get(jobType);
}

export function getRegisteredJobTypes(): string[] {
  return Array.from(handlers.keys());
}
