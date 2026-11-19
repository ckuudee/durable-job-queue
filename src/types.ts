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
  /**
   * While running: a fencing token unique to this claim. `heartbeat`,
   * `complete` and `fail` take it, so a holder whose claim was superseded is
   * rejected even when the new claim has the same `workerId` (another slot of
   * the same Worker, or a replica configured with a shared id). Null otherwise.
   */
  leaseToken: string | null;
  /** Most recent failure, or a note that the previous worker's lease expired. */
  error: string | null;
  /** Timestamps are epoch milliseconds. */
  createdAt: number;
  /** Earliest time a pending job may be claimed. Pushed forward by retry backoff. */
  runAt: number;
  /**
   * Caller-chosen key that makes enqueueing safe to repeat, unique within the
   * queue. Also stable across every attempt, so a handler can pass it to a
   * downstream API that supports idempotency keys.
   */
  idempotencyKey: string | null;
  /** How long a claim or heartbeat extends the worker's hold on the job. */
  leaseMs: number;
  /** While running: when the hold lapses and another worker may take the job. Null otherwise. */
  leaseUntil: number | null;
  claimedAt: number | null;
  finishedAt: number | null;
}

/**
 * A point-in-time count of one queue's jobs, split the way an operator reads
 * them. Every count comes from a single query, so they are consistent with
 * each other.
 */
export interface QueueStats {
  queue: string;
  /** Pending and due: waiting only for a free worker. */
  ready: number;
  /** Pending but not yet due: delayed jobs, and retries waiting out their backoff. */
  scheduled: number;
  /** Running with a live lease. */
  running: number;
  /**
   * Running, but the lease has lapsed: the worker has probably died, and the
   * next claim() on this queue will take the job over (or mark it dead).
   */
  stalled: number;
  completed: number;
  dead: number;
  /**
   * How long the oldest ready job has been ready (now minus its `runAt`), or
   * null when no job is ready. Measured from `runAt` rather than `createdAt`, so
   * a delayed job or a retry doesn't look late just because it was scheduled.
   */
  oldestReadyAgeMs: number | null;
  /** The store's clock reading the snapshot was taken at (epoch ms). */
  at: number;
}

export interface NewJob {
  queue: string;
  payload: unknown;
  /** Total attempts allowed, including the first. Defaults to 3. */
  maxAttempts?: number;
  /** Lease length for this job in milliseconds. Defaults to the store's setting (30 s). */
  leaseMs?: number;
  /**
   * Don't run the job before this time (epoch ms or a Date). A time in the past
   * means "run now". Mutually exclusive with `delayMs`.
   */
  runAt?: number | Date;
  /** Don't run the job until this many milliseconds from now. Mutually exclusive with `runAt`. */
  delayMs?: number;
  /**
   * Makes enqueue idempotent within `queue`: if a job with this key already
   * exists (in any state), that job is returned and nothing new is created.
   * Reusing a key with a different payload throws IdempotencyConflictError.
   */
  idempotencyKey?: string;
}
