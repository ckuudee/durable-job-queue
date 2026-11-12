/**
 * pending   waiting to run (possibly after a retry delay; see `runAt`)
 * running   claimed by a worker
 * completed finished successfully
 * dead      failed every allowed attempt (the dead-letter queue)
 */
export type JobStatus = "pending" | "running" | "completed" | "dead";

export interface Job {
  id: string;
  queue: string;
  /** Any JSON-serializable value supplied at enqueue time. */
  payload: unknown;
  status: JobStatus;
  /** Number of times this job has been claimed by a worker. */
  attempts: number;
  /** The job goes to `dead` when attempt number `maxAttempts` fails. */
  maxAttempts: number;
  /** Worker currently holding (or, for dead jobs, last holding) the job. */
  workerId: string | null;
  /** Failure message from the most recent failed attempt. */
  error: string | null;
  /** Timestamps are epoch milliseconds. */
  createdAt: number;
  /** Earliest time a pending job may be claimed. Pushed forward by retry backoff. */
  runAt: number;
  claimedAt: number | null;
  finishedAt: number | null;
}

export interface NewJob {
  queue: string;
  payload: unknown;
  /** Total attempts allowed, including the first. Defaults to 3. */
  maxAttempts?: number;
}
