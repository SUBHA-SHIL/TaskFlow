
import { completeJob } from './job-completion.repository.js';
import { failJob } from './job-failure.repository.js';
import { claimJob } from './job.repository.js';
import { renewJobLease } from './lease.repository.js';
import { getHandler } from '../handlers/handler.registry.js';

const LEASE_RENEWAL_INTERVAL_MS = 20_000;

export type JobExecutionResult =
  | { status: 'not_claimed' }
  | { status: 'completed' }
  | { status: 'retry_scheduled'; retryAt: Date }
  | { status: 'dead' }
  | { status: 'ownership_lost' };

// Periodically renew the lease while a handler is executing.
function startLeaseRenewal(jobId: string, workerId: string) {
  const controller = new AbortController();

  let ownershipLost = false;
  let renewalInFlight: Promise<void> | null = null;

  const timer = setInterval(() => {
    // Prevent overlapping renewal requests.
    if (renewalInFlight) {
      return;
    }

    renewalInFlight = (async () => {
      try {
        const renewed = await renewJobLease(jobId, workerId);

        if (!renewed) {
          ownershipLost = true;

          controller.abort(
            new Error(`Lease ownership lost for job ${jobId}`),
          );
        }
      } catch (error) {
        ownershipLost = true;

        controller.abort(
          error instanceof Error
            ? error
            : new Error(String(error)),
        );

        console.error('Job lease renewal failed:', {
          jobId,
          workerId,
          error,
        });
      } finally {
        renewalInFlight = null;
      }
    })();
  }, LEASE_RENEWAL_INTERVAL_MS);

  return {
    signal: controller.signal,

    async stop(): Promise<boolean> {
      clearInterval(timer);

      // Wait for an already-running renewal to finish.
      if (renewalInFlight) {
        await renewalInFlight;
      }

      return !ownershipLost;
    },
  };
}

export async function executeJob(
  jobId: string,
  workerId: string,
): Promise<JobExecutionResult> {
  // 1. Atomically claim the job.
  const job = await claimJob(jobId, workerId);

  if (!job) {
    return { status: 'not_claimed' };
  }

  // 2. Find the handler registered for this job type.
  const handler = getHandler(job.jobType);

  // 3. Handle an unknown job type as a processing failure.
  if (!handler) {
    const failure = await failJob(
      job.jobId,
      workerId,
      new Error(`No handler registered for job type: ${job.jobType}`),
    );

    if (!failure) {
      return { status: 'ownership_lost' };
    }

    if (failure.status === 'dead') {
      return { status: 'dead' };
    }

    return {
      status: 'retry_scheduled',
      retryAt: failure.retryAt!,
    };
  }

  // 4. Start renewing the job lease.
  const lease = startLeaseRenewal(job.jobId, workerId);

  // 5. Execute the registered handler.
  let result: unknown;

  try {
    result = await handler(job.payload, {
      jobId: job.jobId,
      workerId,
      attempt: job.attempts,
      signal: lease.signal,
    });
  } catch (error) {
    // Stop renewal before recording the failure.
    const stillOwnsJob = await lease.stop();

    if (!stillOwnsJob) {
      return { status: 'ownership_lost' };
    }

    // 6. Record the failure and schedule a retry if allowed.
    const failure = await failJob(job.jobId, workerId, error);

    if (!failure) {
      return { status: 'ownership_lost' };
    }

    if (failure.status === 'dead') {
      return { status: 'dead' };
    }

    return {
      status: 'retry_scheduled',
      retryAt: failure.retryAt!,
    };
  }

  // 7. Stop renewal and verify ownership.
  const stillOwnsJob = await lease.stop();

  if (!stillOwnsJob) {
    return { status: 'ownership_lost' };
  }

  // 8. Persist the successful result.
  const completed = await completeJob(job.jobId, workerId, result);

  if (!completed) {
    return { status: 'ownership_lost' };
  }

  return { status: 'completed' };
}
