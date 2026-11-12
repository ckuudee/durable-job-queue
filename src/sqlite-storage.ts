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
export const DEFAULT_LEASE_MS = 30_000;

// Bump when SCHEMA_SQL changes. There are no migrations yet: a database file
// with a different version is rejected with a clear error instead of failing
// later with a confusing "no such column".
const SCHEMA_VERSION = 3;

const SCHEMA_SQL = `
CREATE TABLE jobs (
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
  lease_ms     INTEGER NOT NULL CHECK (lease_ms >= 1),
  lease_until  INTEGER,                            -- set only while running
  claimed_at   INTEGER,
  finished_at  INTEGER
);

CREATE INDEX idx_jobs_claim ON jobs(queue, status, run_at, seq);
CREATE INDEX idx_jobs_lease ON jobs(queue, status, lease_until);
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
  lease_ms: number;
  lease_until: number | null;
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
    leaseMs: row.lease_ms,
    leaseUntil: row.lease_until,
    claimedAt: row.claimed_at,
    finishedAt: row.finished_at,
  };
}

function assertLeaseMs(leaseMs: number): void {
  if (!Number.isInteger(leaseMs) || leaseMs < 1) {
    throw new RangeError("leaseMs must be a positive integer");
  }
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
  /** Default lease length for jobs that don't set their own. Defaults to 30 s. */
  leaseMs?: number;
}

export class SqliteStorage implements Storage {
  private readonly db: Database.Database;
  private readonly now: () => number;
  private readonly retry: RetryPolicy;
  private readonly random: () => number;
  private readonly defaultLeaseMs: number;

  private readonly insertJob: Database.Statement;
  private readonly expireExhausted: Database.Statement;
  private readonly claimNext: Database.Statement;
  private readonly extendLease: Database.Statement;
  private readonly markCompleted: Database.Statement;
  private readonly selectHeld: Database.Statement;
  private readonly markRetry: Database.Statement;
  private readonly markDead: Database.Statement;
  private readonly selectDead: Database.Statement;
  private readonly markRequeued: Database.Statement;
  private readonly selectById: Database.Statement;
  private readonly claimTx: Database.Transaction<(workerId: string, queue: string) => JobRow | undefined>;
  private readonly failTx: Database.Transaction<(id: string, workerId: string, error: string) => JobRow>;

  constructor({
    path,
    now = Date.now,
    retry,
    random = Math.random,
    leaseMs = DEFAULT_LEASE_MS,
  }: SqliteStorageOptions) {
    this.retry = { ...DEFAULT_RETRY_POLICY, ...retry };
    validateRetryPolicy(this.retry);
    assertLeaseMs(leaseMs);
    this.now = now;
    this.random = random;
    this.defaultLeaseMs = leaseMs;

    if (path !== ":memory:") mkdirSync(dirname(path), { recursive: true });
    this.db = new Database(path);
    // WAL lets readers and one writer coexist; busy_timeout makes competing
    // processes wait for the write lock instead of failing immediately.
    this.db.pragma("journal_mode = WAL");
    this.db.pragma("busy_timeout = 5000");
    try {
      this.initSchema(path);
    } catch (err) {
      this.db.close(); // a constructor that throws can't be close()d by the caller
      throw err;
    }

    this.insertJob = this.db.prepare(
      `INSERT INTO jobs (id, queue, payload, status, max_attempts, created_at, run_at, lease_ms)
       VALUES (?, ?, ?, 'pending', ?, ?, ?, ?)`,
    );
    // A job whose lease lapsed and that has used every attempt is not run again.
    this.expireExhausted = this.db.prepare(
      `UPDATE jobs
          SET status = 'dead', finished_at = ?, lease_until = NULL,
              error = 'lease expired (worker ' || worker_id || ') after ' || attempts || ' attempts'
        WHERE queue = ? AND status = 'running' AND lease_until <= ? AND attempts >= max_attempts`,
    );
    // One UPDATE ... WHERE id = (SELECT ...) is a single atomic statement, so two
    // claimers can never both pick the same row. A row is claimable when it is
    // pending and due, or running with a lapsed lease (its worker is presumed
    // dead). In an UPDATE, SQLite evaluates every SET expression against the old
    // row, so the CASE below still sees the previous status.
    this.claimNext = this.db.prepare(`
      UPDATE jobs
         SET error = CASE WHEN status = 'running'
                          THEN 'lease expired (worker ' || worker_id || ')'
                          ELSE error END,
             status = 'running', worker_id = ?, claimed_at = ?,
             attempts = attempts + 1, lease_until = ? + lease_ms
       WHERE id = (
         SELECT id FROM jobs
          WHERE queue = ?
            AND ((status = 'pending' AND run_at <= ?) OR (status = 'running' AND lease_until <= ?))
          ORDER BY run_at, seq
          LIMIT 1
       )
      RETURNING *`);
    this.extendLease = this.db.prepare(
      `UPDATE jobs SET lease_until = ? + lease_ms
        WHERE id = ? AND worker_id = ? AND status = 'running'
       RETURNING *`,
    );
    this.markCompleted = this.db.prepare(
      `UPDATE jobs SET status = 'completed', finished_at = ?, lease_until = NULL
        WHERE id = ? AND worker_id = ? AND status = 'running'`,
    );
    this.selectHeld = this.db.prepare(
      "SELECT * FROM jobs WHERE id = ? AND worker_id = ? AND status = 'running'",
    );
    // Retrying releases the worker's hold so the job looks like any other pending job.
    this.markRetry = this.db.prepare(
      `UPDATE jobs
          SET status = 'pending', error = ?, run_at = ?,
              worker_id = NULL, claimed_at = NULL, lease_until = NULL
        WHERE id = ?`,
    );
    this.markDead = this.db.prepare(
      "UPDATE jobs SET status = 'dead', error = ?, finished_at = ?, lease_until = NULL WHERE id = ?",
    );
    this.selectDead = this.db.prepare(
      "SELECT * FROM jobs WHERE queue = ? AND status = 'dead' ORDER BY finished_at, seq LIMIT ?",
    );
    this.markRequeued = this.db.prepare(
      `UPDATE jobs
          SET status = 'pending', attempts = 0, run_at = ?,
              worker_id = NULL, claimed_at = NULL, finished_at = NULL, lease_until = NULL
        WHERE id = ? AND status = 'dead'`,
    );
    this.selectById = this.db.prepare("SELECT * FROM jobs WHERE id = ?");

    // Two statements that must not interleave with another process, so they run
    // as one transaction. Both transactions are called with .immediate(), which
    // takes the write lock up front. That matters most for failTx below: it
    // starts with a SELECT, and a plain (deferred) transaction that reads and
    // then writes fails with SQLITE_BUSY_SNAPSHOT if another process wrote in
    // between; busy_timeout does not retry that error. claimTx starts with a
    // write, so it would take the lock immediately anyway (tests confirm
    // removing .immediate() there changes nothing); it is kept for symmetry.
    this.claimTx = this.db.transaction((workerId: string, queue: string): JobRow | undefined => {
      const now = this.now();
      this.expireExhausted.run(now, queue, now);
      return this.claimNext.get(workerId, now, now, queue, now, now) as JobRow | undefined;
    });

    // Read the attempt count, then decide retry vs dead, then write.
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

  /** Create the schema on a fresh file; refuse a file written by a different schema version. */
  private initSchema(path: string): void {
    this.db
      .transaction(() => {
        const hasJobs = this.db
          .prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'jobs'")
          .get();
        const version = this.db.pragma("user_version", { simple: true }) as number;
        if (hasJobs) {
          if (version !== SCHEMA_VERSION) {
            throw new Error(
              `database ${path} has schema version ${version}, expected ${SCHEMA_VERSION}; ` +
                "there are no migrations yet, so delete the file to recreate it",
            );
          }
          return;
        }
        this.db.exec(SCHEMA_SQL);
        this.db.pragma(`user_version = ${SCHEMA_VERSION}`);
      })
      .immediate();
  }

  async enqueue({
    queue,
    payload,
    maxAttempts = DEFAULT_MAX_ATTEMPTS,
    leaseMs = this.defaultLeaseMs,
  }: NewJob): Promise<Job> {
    if (queue.trim() === "") throw new RangeError("queue must be a non-empty string");
    if (!Number.isInteger(maxAttempts) || maxAttempts < 1) {
      throw new RangeError("maxAttempts must be a positive integer");
    }
    assertLeaseMs(leaseMs);
    const json = JSON.stringify(payload);
    if (json === undefined) throw new TypeError("payload must be JSON-serializable");

    const id = randomUUID();
    const now = this.now();
    this.insertJob.run(id, queue, json, maxAttempts, now, now, leaseMs);
    return toJob(this.selectById.get(id) as JobRow);
  }

  async claim(queue: string, workerId: string): Promise<Job | null> {
    const row = this.claimTx.immediate(workerId, queue);
    return row ? toJob(row) : null;
  }

  async heartbeat(id: string, workerId: string): Promise<Job> {
    const row = this.extendLease.get(this.now(), id, workerId) as JobRow | undefined;
    if (!row) throw new JobStateError(`job ${id} is not running under worker ${workerId}`);
    return toJob(row);
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
