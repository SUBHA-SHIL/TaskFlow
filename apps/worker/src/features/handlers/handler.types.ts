export interface JobHandlerContext {
  jobId: string;
  workerId: string;
  attempt: number;

  // Stable across retries and worker reassignment.
  idempotencyKey: string;

  signal: AbortSignal;
}

export type JobHandler = (
  payload: Record<string, unknown>,
  context: JobHandlerContext,
) => Promise<unknown>;