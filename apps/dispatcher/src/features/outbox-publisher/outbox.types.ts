export interface OutboxEvent {
  outboxId: string;
  aggregateId: string;
  eventType: string;
  payload: {
    jobId: string;
  };
  attempts: number;
}