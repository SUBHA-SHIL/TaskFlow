import { recoverExpiredJobs } from './lease-recovery.repository.js';
import { markStaleWorkersLost } from '../execution/stale-worker.repository.js';

const RECOVERY_INTERVAL_MS = 10_000;

export function startLeaseRecovery() {
    let stopped = false;

    let recoveryInFlight: Promise<void> | null = null;

    async function runRecovery(): Promise<void> {
        if (stopped || recoveryInFlight) {
            return;
        }

        recoveryInFlight = (async () => {
            try {
                const lostWorkerCount = await markStaleWorkersLost();

                if (lostWorkerCount > 0) {
                    console.warn('Stale worker detection completed:', {
                        lostWorkerCount,
                    });
                }
                const recoveredCount = await recoverExpiredJobs(10);

                if (recoveredCount > 0) {
                    console.log('Expired jobs recovered:', recoveredCount);
                }
            } catch (error) {
                console.error('Lease recovery failed:', error);
            } finally {
                recoveryInFlight = null;
            }
        })();

        await recoveryInFlight;
    }

    // Run once immediately, then periodically.
    void runRecovery();

    const timer = setInterval(() => {
        void runRecovery();
    }, RECOVERY_INTERVAL_MS);

    console.log('Lease recovery service started.', {
        intervalMs: RECOVERY_INTERVAL_MS,
    });

    return {
        async stop(): Promise<void> {
            stopped = true;
            clearInterval(timer);

            // Wait for an active recovery transaction to finish.
            if (recoveryInFlight) {
                await recoveryInFlight;
            }

            console.log('Lease recovery service stopped.');
        },
    };
}
