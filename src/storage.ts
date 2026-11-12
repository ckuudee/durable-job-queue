import type { Job, NewJob } from "./types.js";

/** Thrown when a worker acts on a job it does not hold, or a job is in the wrong state. */
export class JobStateError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "JobStateError";
  }
}

/**
 * Persistence contract for the queue. Methods are async so a networked backend
 * (PostgreSQL) can implement the same interface as the embedded SQLite one.
 *
 * The one guarantee every backend must provide: a pending job is handed to
 * exactly one worker, even when many workers call `claim` at the same time.
 */
export interface Storage {
  /** Add a job in `pending` state. */
  enqueue(input: NewJob): Promise<Job>;

  /**
   * Atomically take the oldest ready job in `queue` for `workerId`, moving it
   * to `running`. A job is ready when it is pending and its `runAt` has passed.
   * Returns null when nothing is ready.
   */
  claim(queue: string, workerId: string): Promise<Job | null>;

  /** Mark a running job done. Throws JobStateError unless `workerId` holds it. */
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
