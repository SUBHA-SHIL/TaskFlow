export interface JobHandlerContext {
  jobId: string;
  workerId: string;
  attempt: number;
  signal: AbortSignal;
}

export type JobHandler = (
  payload: Record<string, unknown>,
  context: JobHandlerContext,
) => Promise<unknown>;
