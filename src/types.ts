/**
 * pending   waiting to run (possibly after a retry delay; see `runAt`)
 * running   claimed by a worker, who holds it until `leaseUntil`
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
  /** The job goes to `dead` when attempt number `maxAttempts` fails or is abandoned. */
  maxAttempts: number;
  /** Worker currently holding (or, for dead jobs, last holding) the job. */
  workerId: string | null;
  /** Most recent failure, or a note that the previous worker's lease expired. */
  error: string | null;
  /** Timestamps are epoch milliseconds. */
  createdAt: number;
  /** Earliest time a pending job may be claimed. Pushed forward by retry backoff. */
  runAt: number;
  /** How long a claim or heartbeat extends the worker's hold on the job. */
  leaseMs: number;
  /** While running: when the hold lapses and another worker may take the job. Null otherwise. */
  leaseUntil: number | null;
  claimedAt: number | null;
  finishedAt: number | null;
}

export interface NewJob {
  queue: string;
  payload: unknown;
  /** Total attempts allowed, including the first. Defaults to 3. */
  maxAttempts?: number;
  /** Lease length for this job in milliseconds. Defaults to the store's setting (30 s). */
  leaseMs?: number;
}
