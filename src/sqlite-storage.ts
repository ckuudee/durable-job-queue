import { randomUUID } from "node:crypto";
import { mkdirSync } from "node:fs";
import { dirname } from "node:path";
import Database from "better-sqlite3";
import { JobStateError, type Storage } from "./storage.js";
import type { Job, JobStatus, NewJob } from "./types.js";

const SCHEMA_SQL = `
CREATE TABLE IF NOT EXISTS jobs (
  seq         INTEGER PRIMARY KEY AUTOINCREMENT,  -- FIFO order
  id          TEXT    NOT NULL UNIQUE,
  queue       TEXT    NOT NULL,
  payload     TEXT    NOT NULL,                   -- JSON
  status      TEXT    NOT NULL CHECK (status IN ('pending', 'running', 'completed', 'failed')),
  attempts    INTEGER NOT NULL DEFAULT 0,
  worker_id   TEXT,
  error       TEXT,
  created_at  INTEGER NOT NULL,
  claimed_at  INTEGER,
  finished_at INTEGER
);

CREATE INDEX IF NOT EXISTS idx_jobs_claim ON jobs(queue, status, seq);
`;

interface JobRow {
  id: string;
  queue: string;
  payload: string;
  status: JobStatus;
  attempts: number;
  worker_id: string | null;
  error: string | null;
  created_at: number;
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
    workerId: row.worker_id,
    error: row.error,
    createdAt: row.created_at,
    claimedAt: row.claimed_at,
    finishedAt: row.finished_at,
  };
}

export interface SqliteStorageOptions {
  /** File path, or ":memory:". */
  path: string;
  /** Injectable clock (epoch ms) so tests control time. */
  now?: () => number;
}

export class SqliteStorage implements Storage {
  private readonly db: Database.Database;
  private readonly now: () => number;

  private readonly insertJob: Database.Statement;
  private readonly claimNext: Database.Statement;
  private readonly markCompleted: Database.Statement;
  private readonly markFailed: Database.Statement;
  private readonly selectById: Database.Statement;

  constructor({ path, now = Date.now }: SqliteStorageOptions) {
    if (path !== ":memory:") mkdirSync(dirname(path), { recursive: true });
    this.now = now;
    this.db = new Database(path);
    // WAL lets readers and one writer coexist; busy_timeout makes competing
    // processes wait for the write lock instead of failing immediately.
    this.db.pragma("journal_mode = WAL");
    this.db.pragma("busy_timeout = 5000");
    this.db.exec(SCHEMA_SQL);

    this.insertJob = this.db.prepare(
      "INSERT INTO jobs (id, queue, payload, status, created_at) VALUES (?, ?, ?, 'pending', ?)",
    );
    // A single UPDATE ... WHERE id = (SELECT ...) is one atomic statement: SQLite
    // takes the write lock before the subselect runs, so two claimers can never
    // both see the same row as pending.
    this.claimNext = this.db.prepare(`
      UPDATE jobs
         SET status = 'running', worker_id = ?, claimed_at = ?, attempts = attempts + 1
       WHERE id = (
         SELECT id FROM jobs
          WHERE queue = ? AND status = 'pending'
          ORDER BY seq
          LIMIT 1
       )
      RETURNING *`);
    this.markCompleted = this.db.prepare(
      "UPDATE jobs SET status = 'completed', finished_at = ? WHERE id = ? AND worker_id = ? AND status = 'running'",
    );
    this.markFailed = this.db.prepare(
      "UPDATE jobs SET status = 'failed', error = ?, finished_at = ? WHERE id = ? AND worker_id = ? AND status = 'running'",
    );
    this.selectById = this.db.prepare("SELECT * FROM jobs WHERE id = ?");
  }

  async enqueue({ queue, payload }: NewJob): Promise<Job> {
    if (queue.trim() === "") throw new RangeError("queue must be a non-empty string");
    const json = JSON.stringify(payload);
    if (json === undefined) throw new TypeError("payload must be JSON-serializable");

    const id = randomUUID();
    this.insertJob.run(id, queue, json, this.now());
    return toJob(this.selectById.get(id) as JobRow);
  }

  async claim(queue: string, workerId: string): Promise<Job | null> {
    const row = this.claimNext.get(workerId, this.now(), queue) as JobRow | undefined;
    return row ? toJob(row) : null;
  }

  async complete(id: string, workerId: string): Promise<void> {
    const { changes } = this.markCompleted.run(this.now(), id, workerId);
    if (changes === 0) throw new JobStateError(`job ${id} is not running under worker ${workerId}`);
  }

  async fail(id: string, workerId: string, error: string): Promise<void> {
    const { changes } = this.markFailed.run(error, this.now(), id, workerId);
    if (changes === 0) throw new JobStateError(`job ${id} is not running under worker ${workerId}`);
  }

  async get(id: string): Promise<Job | null> {
    const row = this.selectById.get(id) as JobRow | undefined;
    return row ? toJob(row) : null;
  }

  async close(): Promise<void> {
    this.db.close();
  }
}
