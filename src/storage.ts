import type { Job, NewJob } from "./types.js";

/** Thrown when a worker acts on a job it does not hold (wrong worker, or not running). */
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
   * Atomically take the oldest pending job in `queue` for `workerId`, moving it
   * to `running`. Returns null when nothing is pending.
   */
  claim(queue: string, workerId: string): Promise<Job | null>;

  /** Mark a running job done. Throws JobStateError unless `workerId` holds it. */
  complete(id: string, workerId: string): Promise<void>;

  /** Mark a running job failed. Throws JobStateError unless `workerId` holds it. */
  fail(id: string, workerId: string, error: string): Promise<void>;

  get(id: string): Promise<Job | null>;

  close(): Promise<void>;
}
