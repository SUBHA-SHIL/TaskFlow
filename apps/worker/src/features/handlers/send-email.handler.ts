
import type {
  JobHandler,
  JobHandlerContext,
} from './handler.types.js';

export const sendEmailHandler: JobHandler = async (
  payload: Record<string, unknown>,
  context: JobHandlerContext,
) => {
  const { to, subject, body } = payload;

  if (
    typeof to !== 'string' ||
    typeof subject !== 'string' ||
    typeof body !== 'string'
  ) {
    throw new Error(
      'Invalid email payload: to, subject, and body must be strings',
    );
  }

  console.log('Processing email job:', {
    jobId: context.jobId,
    workerId: context.workerId,
    attempt: context.attempt,
    to,
    subject,
  });

  // Simulated email delivery.
  console.log(`Simulated email sent to ${to}`);

  return {
    success: true,
    recipient: to,
    subject,
    message: 'Email processed successfully',
    processedAt: new Date().toISOString(),
  };
};
