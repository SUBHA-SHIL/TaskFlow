import { updateWorkerHeartbeat } from './worker.repository.js';

const HEARTBEAT_INTERVAL_MS = 10_000;

export function startWorkerHeartbeat(
  workerId: string,
  onFailure: (error: unknown) => void,
) {
  let stopped = false;
  let failureReported = false;
  let heartbeatInFlight: Promise<void> | null = null;

  async function sendHeartbeat(): Promise<void> {
    if (stopped || heartbeatInFlight) {
      return;
    }

    let heartbeatError: unknown;

    heartbeatInFlight = (async () => {
      try {
        await updateWorkerHeartbeat(workerId);
      } catch (error) {
        heartbeatError = error;
        stopped = true;

        console.error('Worker heartbeat failed:', {
          workerId,
          error,
        });
      } finally {
        heartbeatInFlight = null;
      }
    })();

    await heartbeatInFlight;

    if (heartbeatError !== undefined && !failureReported) {
      failureReported = true;
      clearInterval(timer);
      onFailure(heartbeatError);
    }
  }

  const timer = setInterval(() => {
    void sendHeartbeat();
  }, HEARTBEAT_INTERVAL_MS);

  console.log('Worker heartbeat service started.', {
    workerId,
    intervalMs: HEARTBEAT_INTERVAL_MS,
  });

  // Send the first heartbeat immediately.
  void sendHeartbeat();

  return {
    async stop(): Promise<void> {
      stopped = true;
      clearInterval(timer);

      if (heartbeatInFlight) {
        await heartbeatInFlight;
      }

      console.log('Worker heartbeat service stopped.', {
        workerId,
      });
    },
  };
}
