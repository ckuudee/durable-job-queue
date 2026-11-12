import type { Job, NewJob } from "./types.js";

/** Thrown when a worker acts on a job it does not hold, or a job is in the wrong state. */
export class JobStateError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "JobStateError";
  }
}

/** Thrown when an idempotency key is reused with a different payload: almost certainly a caller bug. */
export class IdempotencyConflictError extends Error {
  constructor(queue: string, key: string) {
    super(`idempotency key "${key}" in queue "${queue}" was already used with a different payload`);
    this.name = "IdempotencyConflictError";
  }
}

/**
 * Persistence contract for the queue. Methods are async so a networked backend
 * (PostgreSQL) can implement the same interface as the embedded SQLite one.
 *
 * Guarantees every backend must provide:
 * - A ready job is handed to exactly one worker, even when many workers call
 *   `claim` at the same time.
 * - A worker holds a claimed job only for its lease. If the worker goes silent
 *   (crash, hang), the lease lapses and the job is handed to another worker.
 *   Delivery is therefore at-least-once: a job can run more than once.
 * - A worker that lost its job (lease lapsed and the job was re-claimed) can no
 *   longer complete, fail or heartbeat it: those throw JobStateError.
 */
export interface Storage {
  /**
   * Add a job in `pending` state. With an `idempotencyKey`, a repeat call for
   * the same (queue, key) returns the existing job in whatever state it is now,
   * including completed or dead, and creates nothing: a producer that retries
   * after a timeout cannot cause a duplicate. The first call's `maxAttempts` and
   * `leaseMs` win. Exactly one of any number of concurrent callers creates the job.
   */
  enqueue(input: NewJob): Promise<Job>;

  /**
   * Atomically take the oldest ready job in `queue` for `workerId`, moving it
   * to `running` with a fresh lease. A job is ready when it is pending and its
   * `runAt` has passed, or when it is running and its lease has lapsed (the
   * previous worker is presumed dead). A lapsed job that has already used all
   * its attempts is marked dead instead. Returns null when nothing is ready.
   */
  claim(queue: string, workerId: string): Promise<Job | null>;

  /**
   * Extend the lease of a job `workerId` holds by the job's `leaseMs`. Workers
   * call this periodically while a job runs; a JobStateError means the job was
   * lost and the worker should stop working on it.
   */
  heartbeat(id: string, workerId: string): Promise<Job>;

  /**
   * Mark a running job done. Throws JobStateError unless `workerId` holds it.
   * A worker whose lease lapsed can still complete, as long as nobody has
   * re-claimed the job yet.
   */
  complete(id: string, workerId: string): Promise<void>;

  /**
   * Record a failed attempt. If attempts remain, the job returns to `pending`
   * with `runAt` pushed back by the retry backoff; otherwise it becomes `dead`.
   * Returns the updated job so the caller can see which happened. Throws
   * JobStateError unless `workerId` holds the job.
   */
  fail(id: string, workerId: string, error: string): Promise<Job>;

  /** Dead jobs in `queue`, oldest failure first. */
  listDead(queue: string, limit?: number): Promise<Job[]>;

  /** Put a dead job back in the queue with a fresh attempt count. Throws JobStateError unless it is dead. */
  requeueDead(id: string): Promise<Job>;

  get(id: string): Promise<Job | null>;

  close(): Promise<void>;
}
