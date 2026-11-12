export type JobStatus = "pending" | "running" | "completed" | "failed";

export interface Job {
  id: string;
  queue: string;
  /** Any JSON-serializable value supplied at enqueue time. */
  payload: unknown;
  status: JobStatus;
  /** Number of times this job has been claimed by a worker. */
  attempts: number;
  /** Worker currently holding (or last holding) the job. */
  workerId: string | null;
  /** Failure message from the most recent failed attempt. */
  error: string | null;
  /** Timestamps are epoch milliseconds. */
  createdAt: number;
  claimedAt: number | null;
  finishedAt: number | null;
}

export interface NewJob {
  queue: string;
  payload: unknown;
}
