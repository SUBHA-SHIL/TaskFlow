import type { JobHandler } from './handler.types.js';

export const crashTestHandler: JobHandler = async (_payload, context) => {
  console.log('CRASH_TEST_HANDLER_STARTED', {
    jobId: context.jobId,
    workerId: context.workerId,
    attempt: context.attempt,
  });

  // First attempt deliberately remains in execution.
  if (context.attempt === 1) {
    await new Promise<never>((_, reject) => {
      const abort = () => {
        reject(context.signal.reason ?? new Error('Crash test execution aborted'));
      };

      if (context.signal.aborted) {
        abort();
        return;
      }

      context.signal.addEventListener('abort', abort, {
        once: true,
      });
    });
  }

  // The recovered attempt completes successfully.
  return {
    recovered: true,
    completedBy: context.workerId,
    attempt: context.attempt,
  };
};
