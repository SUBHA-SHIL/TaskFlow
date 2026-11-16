import type { JobMessage } from '@taskflow/messaging';

import { executeJob } from './job-execution.service.js';

export async function handleJobMessage(
  message: JobMessage,
  workerId: string,
): Promise<void> {
  console.log('Received job message:', {
    jobId: message.jobId,
    jobType: message.jobType,
  });

  const result = await executeJob(message.jobId, workerId);

  console.log(`Job ${message.jobId} processing result:`, result);
}
