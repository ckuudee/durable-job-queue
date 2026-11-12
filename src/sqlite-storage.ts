import { randomUUID } from "node:crypto";
import { mkdirSync } from "node:fs";
import { dirname } from "node:path";
import Database from "better-sqlite3";
import {
  DEFAULT_RETRY_POLICY,
  computeBackoff,
  validateRetryPolicy,
  type RetryPolicy,
} from "./backoff.js";
import { JobStateError, type Storage } from "./storage.js";
import type { Job, JobStatus, NewJob } from "./types.js";

const DEFAULT_MAX_ATTEMPTS = 3;

// No migration support yet: a database file created by an older version of this
// schema must be deleted and recreated.
const SCHEMA_SQL = `
CREATE TABLE IF NOT EXISTS jobs (
  seq          INTEGER PRIMARY KEY AUTOINCREMENT,  -- FIFO tiebreaker
  id           TEXT    NOT NULL UNIQUE,
  queue        TEXT    NOT NULL,
  payload      TEXT    NOT NULL,                   -- JSON
  status       TEXT    NOT NULL CHECK (status IN ('pending', 'running', 'completed', 'dead')),
  attempts     INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL CHECK (max_attempts >= 1),
  worker_id    TEXT,
  error        TEXT,
  created_at   INTEGER NOT NULL,
  run_at       INTEGER NOT NULL,                   -- earliest claim time (retry backoff)
  claimed_at   INTEGER,
  finished_at  INTEGER
);

CREATE INDEX IF NOT EXISTS idx_jobs_claim ON jobs(queue, status, run_at, seq);
`;

interface JobRow {
  id: string;
  queue: string;
  payload: string;
  status: JobStatus;
  attempts: number;
  max_attempts: number;
  worker_id: string | null;
  error: string | null;
  created_at: number;
  run_at: number;
  claimed_at: number | null;
  finished_at: number | null;
}

function toJob(row: JobRow): Job {
  return {
    id: row.id,
    queue: row.queue,
    payload: JSON.parse(row.payload),
    status: row.status,
    attempts: row.attempts,
    maxAttempts: row.max_attempts,
    workerId: row.worker_id,
    error: row.error,
    createdAt: row.created_at,
    runAt: row.run_at,
    claimedAt: row.claimed_at,
    finishedAt: row.finished_at,
  };
}

export interface SqliteStorageOptions {
  /** File path, or ":memory:". */
  path: string;
  /** Injectable clock (epoch ms) so tests control time. */
  now?: () => number;
  /** Overrides for the retry backoff; unspecified fields use DEFAULT_RETRY_POLICY. */
  retry?: Partial<RetryPolicy>;
  /** Injectable randomness for backoff jitter. */
  random?: () => number;
}

export class SqliteStorage implements Storage {
  private readonly db: Database.Database;
  private readonly now: () => number;
  private readonly retry: RetryPolicy;
  private readonly random: () => number;

  private readonly insertJob: Database.Statement;
  private readonly claimNext: Database.Statement;
  private readonly markCompleted: Database.Statement;
  private readonly selectHeld: Database.Statement;
  private readonly markRetry: Database.Statement;
  private readonly markDead: Database.Statement;
  private readonly selectDead: Database.Statement;
  private readonly markRequeued: Database.Statement;
  private readonly selectById: Database.Statement;
  private readonly failTx: Database.Transaction<(id: string, workerId: string, error: string) => JobRow>;

  constructor({ path, now = Date.now, retry, random = Math.random }: SqliteStorageOptions) {
    this.retry = { ...DEFAULT_RETRY_POLICY, ...retry };
    validateRetryPolicy(this.retry);
    this.now = now;
    this.random = random;

    if (path !== ":memory:") mkdirSync(dirname(path), { recursive: true });
    this.db = new Database(path);
    // WAL lets readers and one writer coexist; busy_timeout makes competing
    // processes wait for the write lock instead of failing immediately.
    this.db.pragma("journal_mode = WAL");
    this.db.pragma("busy_timeout = 5000");
    this.db.exec(SCHEMA_SQL);

    this.insertJob = this.db.prepare(
      `INSERT INTO jobs (id, queue, payload, status, max_attempts, created_at, run_at)
       VALUES (?, ?, ?, 'pending', ?, ?, ?)`,
    );
    // A single UPDATE ... WHERE id = (SELECT ...) is one atomic statement: SQLite
    // takes the write lock before the subselect runs, so two claimers can never
    // both see the same row as pending.
    this.claimNext = this.db.prepare(`
      UPDATE jobs
         SET status = 'running', worker_id = ?, claimed_at = ?, attempts = attempts + 1
       WHERE id = (
         SELECT id FROM jobs
          WHERE queue = ? AND status = 'pending' AND run_at <= ?
          ORDER BY run_at, seq
          LIMIT 1
       )
      RETURNING *`);
    this.markCompleted = this.db.prepare(
      "UPDATE jobs SET status = 'completed', finished_at = ? WHERE id = ? AND worker_id = ? AND status = 'running'",
    );
    this.selectHeld = this.db.prepare(
      "SELECT * FROM jobs WHERE id = ? AND worker_id = ? AND status = 'running'",
    );
    // Retrying releases the worker's hold so the job looks like any other pending job.
    this.markRetry = this.db.prepare(
      "UPDATE jobs SET status = 'pending', error = ?, run_at = ?, worker_id = NULL, claimed_at = NULL WHERE id = ?",
    );
    this.markDead = this.db.prepare(
      "UPDATE jobs SET status = 'dead', error = ?, finished_at = ? WHERE id = ?",
    );
    this.selectDead = this.db.prepare(
      "SELECT * FROM jobs WHERE queue = ? AND status = 'dead' ORDER BY finished_at, seq LIMIT ?",
    );
    this.markRequeued = this.db.prepare(
      `UPDATE jobs
          SET status = 'pending', attempts = 0, run_at = ?,
              worker_id = NULL, claimed_at = NULL, finished_at = NULL
        WHERE id = ? AND status = 'dead'`,
    );
    this.selectById = this.db.prepare("SELECT * FROM jobs WHERE id = ?");

    // Read-then-write, so it runs as one transaction. Calling it with
    // .immediate() takes the write lock up front, which keeps two processes
    // from both reading the same attempt count and then deadlocking on upgrade.
    this.failTx = this.db.transaction((id: string, workerId: string, error: string): JobRow => {
      const held = this.selectHeld.get(id, workerId) as JobRow | undefined;
      if (!held) throw new JobStateError(`job ${id} is not running under worker ${workerId}`);

      const now = this.now();
      if (held.attempts >= held.max_attempts) {
        this.markDead.run(error, now, id);
      } else {
        const delay = computeBackoff(held.attempts, this.retry, this.random);
        this.markRetry.run(error, now + delay, id);
      }
      return this.selectById.get(id) as JobRow;
    });
  }

  async enqueue({ queue, payload, maxAttempts = DEFAULT_MAX_ATTEMPTS }: NewJob): Promise<Job> {
    if (queue.trim() === "") throw new RangeError("queue must be a non-empty string");
    if (!Number.isInteger(maxAttempts) || maxAttempts < 1) {
      throw new RangeError("maxAttempts must be a positive integer");
    }
    const json = JSON.stringify(payload);
    if (json === undefined) throw new TypeError("payload must be JSON-serializable");

    const id = randomUUID();
    const now = this.now();
    this.insertJob.run(id, queue, json, maxAttempts, now, now);
    return toJob(this.selectById.get(id) as JobRow);
  }

  async claim(queue: string, workerId: string): Promise<Job | null> {
    const now = this.now();
    const row = this.claimNext.get(workerId, now, queue, now) as JobRow | undefined;
    return row ? toJob(row) : null;
  }

  async complete(id: string, workerId: string): Promise<void> {
    const { changes } = this.markCompleted.run(this.now(), id, workerId);
    if (changes === 0) throw new JobStateError(`job ${id} is not running under worker ${workerId}`);
  }

  async fail(id: string, workerId: string, error: string): Promise<Job> {
    return toJob(this.failTx.immediate(id, workerId, error));
  }

  async listDead(queue: string, limit = 100): Promise<Job[]> {
    return (this.selectDead.all(queue, limit) as JobRow[]).map(toJob);
  }

  async requeueDead(id: string): Promise<Job> {
    const { changes } = this.markRequeued.run(this.now(), id);
    if (changes === 0) throw new JobStateError(`job ${id} is not dead`);
    return toJob(this.selectById.get(id) as JobRow);
  }

  async get(id: string): Promise<Job | null> {
    const row = this.selectById.get(id) as JobRow | undefined;
    return row ? toJob(row) : null;
  }

  async close(): Promise<void> {
    this.db.close();
  }
}
