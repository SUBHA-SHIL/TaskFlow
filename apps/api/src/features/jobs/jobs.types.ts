export interface CreateJobResponse {
    jobId: string;
    status: GetJobResponse['status'];
    jobType: string;
    createdAt: string;
}

export interface GetJobResponse {
    jobId: string;
    jobType: string;
    status:
    | 'pending'
    | 'queued'
    | 'running'
    | 'completed'
    | 'failed'
    | 'retry_scheduled'
    | 'dead'
    | 'cancelled'
    | 'scheduled';
    payload: Record<string, unknown>;
    priority: number;
    attempts: number;
    maxAttempts: number;
    runAt: string;
    result: Record<string, unknown> | null;
    lastError: string | null;
    createdAt: string;
    updatedAt: string;
    completedAt: string | null;
    totalAttempts: number;
}

export interface JobListItem {
    jobId: string;
    jobType: string;
    status: GetJobResponse['status'];
    priority: number;
    attempts: number;
    maxAttempts: number;
    runAt: string;
    createdAt: string;
    updatedAt: string;
}

export interface ListJobsResponse {
    jobs: JobListItem[];
    pagination: {
        page: number;
        limit: number;
        totalItems: number;
        totalPages: number;
        hasNextPage: boolean;
        hasPreviousPage: boolean;
    };
}

export interface CancelJobResponse {
    jobId: string;
    status: string;
    cancellationStatus:
    | 'cancelled'
    | 'cancellation_requested'
    | 'already_cancelled'
    | 'not_cancellable';
}

export interface RetryJobResponse {
    jobId: string;
    status: 'pending';
    previousStatus: 'failed' | 'dead';
    attempts: number;
}


