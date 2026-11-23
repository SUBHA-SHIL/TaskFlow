import { cancelRunningJob } from './job-cancellation.repository.js';
import { completeJob } from './job-completion.repository.js';
import { failJob } from './job-failure.repository.js';
import { claimJob } from './job.repository.js';
import {
  checkJobCancellation,
  renewJobLease,
  type CancellationCheckResult,
  type LeaseRenewalResult,
} from './lease.repository.js';
import { getHandler } from '../handlers/handler.registry.js';

const LEASE_RENEWAL_INTERVAL_MS = 20_000;

export type JobExecutionResult =
  | { status: 'not_claimed' }
  | { status: 'completed' }
  | { status: 'cancelled' }
  | { status: 'retry_scheduled'; retryAt: Date }
  | { status: 'dead' }
  | { status: 'ownership_lost' };


const CANCELLATION_CHECK_INTERVAL_MS = 1_000;

function startLeaseRenewal(jobId: string, workerId: string) {
  const controller = new AbortController();

  let state: LeaseRenewalResult = 'renewed';

  let renewalInFlight: Promise<void> | null = null;
  let cancellationCheckInFlight: Promise<void> | null = null;

  async function handleStateChange(
    result: LeaseRenewalResult | CancellationCheckResult,
    source: string,
  ): Promise<void> {
    if (result === 'active' || result === 'renewed') {
      return;
    }

    if (state !== 'renewed') {
      return;
    }

    state = result;

    controller.abort(
      new Error(
        result === 'cancellation_requested'
          ? `Cancellation requested for job ${jobId}`
          : `Lease ownership lost for job ${jobId}`,
      ),
    );

    if (source === 'lease renewal') {
      console.error('Job lease renewal failed:', {
        jobId,
        workerId,
        result,
      });
    }
  }

  const renewalTimer = setInterval(() => {
    if (renewalInFlight || state !== 'renewed') {
      return;
    }

    renewalInFlight = (async () => {
      try {
        const result = await renewJobLease(jobId, workerId);
        await handleStateChange(result, 'lease renewal');
      } catch (error) {
        state = 'ownership_lost';

        controller.abort(
          error instanceof Error ? error : new Error(String(error)),
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

  const cancellationTimer = setInterval(() => {
    if (cancellationCheckInFlight || state !== 'renewed') {
      return;
    }

    cancellationCheckInFlight = (async () => {
      try {
        const result = await checkJobCancellation(jobId, workerId);
        await handleStateChange(result, 'cancellation check');
      } catch (error) {
        state = 'ownership_lost';

        controller.abort(
          error instanceof Error ? error : new Error(String(error)),
        );

        console.error('Job cancellation check failed:', {
          jobId,
          workerId,
          error,
        });
      } finally {
        cancellationCheckInFlight = null;
      }
    })();
  }, CANCELLATION_CHECK_INTERVAL_MS);

  return {
    signal: controller.signal,

    async stop(): Promise<LeaseRenewalResult> {
      clearInterval(renewalTimer);
      clearInterval(cancellationTimer);

      const pendingOperations = [
        renewalInFlight,
        cancellationCheckInFlight,
      ].filter(
        (operation): operation is Promise<void> => operation !== null,
      );

      await Promise.all(pendingOperations);

      return state;
    },
  };
}


async function persistCancellation(
  jobId: string,
  workerId: string,
): Promise<JobExecutionResult> {
  const cancelled = await cancelRunningJob(jobId, workerId);

  return cancelled
    ? { status: 'cancelled' }
    : { status: 'ownership_lost' };
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

    if (failure.status === 'cancelled') {
      return { status: 'cancelled' };
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
      idempotencyKey: job.jobId,
      signal: lease.signal,
    });
  } catch (error) {
    // Stop renewal before recording the outcome.
    const leaseState = await lease.stop();

    if (leaseState === 'ownership_lost') {
      return { status: 'ownership_lost' };
    }

    if (leaseState === 'cancellation_requested') {
      return persistCancellation(job.jobId, workerId);
    }

    // 6. Record a normal failure and schedule a retry if allowed.
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
  const leaseState = await lease.stop();

  if (leaseState === 'ownership_lost') {
    return { status: 'ownership_lost' };
  }

  if (leaseState === 'cancellation_requested') {
    return persistCancellation(job.jobId, workerId);
  }

  // 8. Persist the successful result.
  const completion = await completeJob(
    job.jobId,
    workerId,
    result,
  );

  if (completion === 'ownership_lost') {
    return { status: 'ownership_lost' };
  }

  if (completion === 'cancellation_requested') {
    return persistCancellation(job.jobId, workerId);
  }

  return { status: 'completed' };
}
