import { randomUUID } from "node:crypto";
import pg from "pg";
import {
  DEFAULT_RETRY_POLICY,
  computeBackoff,
  validateRetryPolicy,
  type RetryPolicy,
} from "./backoff.js";
import { assertIdempotencyKey, canonicalize } from "./idempotency.js";
import { DEFAULT_LEASE_MS, IdempotencyConflictError, JobStateError, type Storage } from "./storage.js";
import type { Job, JobStatus, NewJob } from "./types.js";

const DEFAULT_MAX_ATTEMPTS = 3;

// Bump when schemaSql changes. Like the SQLite backend there are no migrations
// yet: a schema written by a different version is rejected with a clear error.
const SCHEMA_VERSION = 1;

// Schema names are interpolated into SQL (identifiers can't be bound as
// parameters), so only plain lowercase identifiers are accepted. 63 is
// PostgreSQL's identifier length limit.
const SCHEMA_NAME = /^[a-z_][a-z0-9_]{0,62}$/;

function schemaSql(schema: string): string {
  return `
CREATE TABLE ${schema}.jobs (
  seq          BIGINT  GENERATED ALWAYS AS IDENTITY PRIMARY KEY,  -- FIFO tiebreaker
  id           TEXT    NOT NULL UNIQUE,
  queue        TEXT    NOT NULL,
  payload      JSONB   NOT NULL,
  status       TEXT    NOT NULL CHECK (status IN ('pending', 'running', 'completed', 'dead')),
  attempts     INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL CHECK (max_attempts >= 1),
  worker_id    TEXT,
  error        TEXT,
  created_at   BIGINT  NOT NULL,                  -- epoch ms, from the injected clock
  run_at       BIGINT  NOT NULL,                  -- earliest claim time (retry backoff)
  idempotency_key TEXT,                           -- NULL = no deduplication
  lease_ms     INTEGER NOT NULL CHECK (lease_ms >= 1),
  lease_until  BIGINT,                            -- set only while running
  claimed_at   BIGINT,
  finished_at  BIGINT
);

CREATE INDEX jobs_claim_idx ON ${schema}.jobs (queue, status, run_at, seq);
CREATE INDEX jobs_lease_idx ON ${schema}.jobs (queue, status, lease_until);

-- One job per (queue, key), enforced by the database. NULLs are distinct in a
-- PostgreSQL unique index by default, so jobs without a key never collide.
CREATE UNIQUE INDEX jobs_idempotency_idx ON ${schema}.jobs (queue, idempotency_key);

CREATE TABLE ${schema}.jobq_meta (schema_version INTEGER NOT NULL);
INSERT INTO ${schema}.jobq_meta (schema_version) VALUES (${SCHEMA_VERSION});
`;
}

/**
 * A row as node-postgres returns it. BIGINT columns arrive as strings, because
 * a 64-bit integer may not fit in a JS number; epoch milliseconds always do, so
 * toJob converts them. JSONB arrives already parsed.
 */
interface PgRow {
  id: string;
  queue: string;
  payload: unknown;
  status: JobStatus;
  attempts: number;
  max_attempts: number;
  worker_id: string | null;
  error: string | null;
  created_at: string;
  run_at: string;
  idempotency_key: string | null;
  lease_ms: number;
  lease_until: string | null;
  claimed_at: string | null;
  finished_at: string | null;
}

function toNumberOrNull(value: string | null): number | null {
  return value === null ? null : Number(value);
}

function toJob(row: PgRow): Job {
  return {
    id: row.id,
    queue: row.queue,
    payload: row.payload,
    status: row.status,
    attempts: row.attempts,
    maxAttempts: row.max_attempts,
    workerId: row.worker_id,
    error: row.error,
    createdAt: Number(row.created_at),
    runAt: Number(row.run_at),
    idempotencyKey: row.idempotency_key,
    leaseMs: row.lease_ms,
    leaseUntil: toNumberOrNull(row.lease_until),
    claimedAt: toNumberOrNull(row.claimed_at),
    finishedAt: toNumberOrNull(row.finished_at),
  };
}

function assertLeaseMs(leaseMs: number): void {
  if (!Number.isInteger(leaseMs) || leaseMs < 1) {
    throw new RangeError("leaseMs must be a positive integer");
  }
}

export interface PostgresStorageOptions {
  /** e.g. postgres://user:password@localhost:5432/jobs */
  connectionString: string;
  /**
   * PostgreSQL schema (namespace) holding the queue's tables, created if
   * missing. Lets several independent queues, or parallel test runs, share one
   * database. Defaults to "public".
   */
  schema?: string;
  /** Maximum pooled connections. Defaults to 10. */
  maxConnections?: number;
  /** Injectable clock (epoch ms) so tests control time. */
  now?: () => number;
  /** Overrides for the retry backoff; unspecified fields use DEFAULT_RETRY_POLICY. */
  retry?: Partial<RetryPolicy>;
  /** Injectable randomness for backoff jitter. */
  random?: () => number;
  /** Default lease length for jobs that don't set their own. Defaults to 30 s. */
  leaseMs?: number;
}

/**
 * Storage backed by PostgreSQL, for many workers on many machines. Create it
 * with `await PostgresStorage.connect(options)`: setting up the schema needs
 * the network, and a constructor can't be async.
 *
 * Every time value is a query parameter taken from the injected clock, never
 * the database's now(), so behavior matches the SQLite backend and the shared
 * contract tests can control time.
 */
export class PostgresStorage implements Storage {
  private readonly pool: pg.Pool;
  private readonly schema: string;
  private readonly jobs: string;
  private readonly now: () => number;
  private readonly retry: RetryPolicy;
  private readonly random: () => number;
  private readonly defaultLeaseMs: number;

  private constructor({
    connectionString,
    schema = "public",
    maxConnections = 10,
    now = Date.now,
    retry,
    random = Math.random,
    leaseMs = DEFAULT_LEASE_MS,
  }: PostgresStorageOptions) {
    if (!SCHEMA_NAME.test(schema)) {
      throw new RangeError("schema must be a lowercase identifier: letters, digits and underscores");
    }
    this.retry = { ...DEFAULT_RETRY_POLICY, ...retry };
    validateRetryPolicy(this.retry);
    assertLeaseMs(leaseMs);
    this.schema = schema;
    this.jobs = `${schema}.jobs`;
    this.now = now;
    this.random = random;
    this.defaultLeaseMs = leaseMs;

    this.pool = new pg.Pool({ connectionString, max: maxConnections });
    // An idle pooled connection can fail (database restart, network drop). The
    // pool discards it and the next query opens a new one, but without a
    // listener the 'error' event would crash the process.
    this.pool.on("error", () => {});
  }

  static async connect(options: PostgresStorageOptions): Promise<PostgresStorage> {
    const store = new PostgresStorage(options);
    try {
      await store.initSchema();
    } catch (err) {
      await store.pool.end(); // the caller never gets a store to close()
      throw err;
    }
    return store;
  }

  /** Create the schema on first use; refuse one written by a different schema version. */
  private async initSchema(): Promise<void> {
    await this.transaction(async (client) => {
      // Serializes setup when several processes start at once against an empty
      // database; otherwise two could both see "no table" and both try to create it.
      // The lock is released automatically at commit or rollback.
      await client.query("SELECT pg_advisory_xact_lock(hashtext($1))", [`durable-job-queue:${this.schema}`]);
      await client.query(`CREATE SCHEMA IF NOT EXISTS ${this.schema}`);

      const { rows } = await client.query<{ jobs: string | null; meta: string | null }>(
        "SELECT to_regclass($1) AS jobs, to_regclass($2) AS meta",
        [this.jobs, `${this.schema}.jobq_meta`],
      );
      const exists = rows[0];
      if (!exists?.jobs) {
        await client.query(schemaSql(this.schema));
        return;
      }
      let version = 0;
      if (exists.meta) {
        const meta = await client.query<{ schema_version: number }>(
          `SELECT schema_version FROM ${this.schema}.jobq_meta`,
        );
        version = meta.rows[0]?.schema_version ?? 0;
      }
      if (version !== SCHEMA_VERSION) {
        throw new Error(
          `schema "${this.schema}" has schema version ${version}, expected ${SCHEMA_VERSION}; ` +
            "there are no migrations yet, so drop the schema to recreate it",
        );
      }
    });
  }

  /**
   * Run `fn` inside BEGIN/COMMIT on one pooled connection. A transaction has to
   * stay on a single connection, so pool.query() (which may use a different
   * connection per call) can't be used for it.
   */
  private async transaction<T>(fn: (client: pg.PoolClient) => Promise<T>): Promise<T> {
    const client = await this.pool.connect();
    let broken = false;
    try {
      await client.query("BEGIN");
      const result = await fn(client);
      await client.query("COMMIT");
      return result;
    } catch (err) {
      try {
        await client.query("ROLLBACK");
      } catch {
        broken = true; // don't hand a connection in an unknown state back to the pool
      }
      throw err;
    } finally {
      client.release(broken);
    }
  }

  async enqueue({
    queue,
    payload,
    maxAttempts = DEFAULT_MAX_ATTEMPTS,
    leaseMs = this.defaultLeaseMs,
    idempotencyKey,
  }: NewJob): Promise<Job> {
    if (queue.trim() === "") throw new RangeError("queue must be a non-empty string");
    if (!Number.isInteger(maxAttempts) || maxAttempts < 1) {
      throw new RangeError("maxAttempts must be a positive integer");
    }
    assertLeaseMs(leaseMs);
    if (idempotencyKey !== undefined) assertIdempotencyKey(idempotencyKey);
    // Serialize here rather than letting node-postgres do it: it would turn a
    // top-level JS array into a PostgreSQL array literal, not JSON.
    const json = JSON.stringify(payload);
    if (json === undefined) throw new TypeError("payload must be JSON-serializable");

    // ON CONFLICT DO NOTHING: if another transaction is inserting the same key
    // right now, PostgreSQL waits for it to finish and then inserts nothing, so
    // exactly one of any number of concurrent callers creates the job.
    const now = this.now();
    const inserted = await this.pool.query<PgRow>(
      `INSERT INTO ${this.jobs}
         (id, queue, payload, status, max_attempts, created_at, run_at, lease_ms, idempotency_key)
       VALUES ($1, $2, $3::jsonb, 'pending', $4, $5::bigint, $5::bigint, $6, $7)
       ON CONFLICT (queue, idempotency_key) DO NOTHING
       RETURNING *`,
      [randomUUID(), queue, json, maxAttempts, now, leaseMs, idempotencyKey ?? null],
    );
    const created = inserted.rows[0];
    if (created) return toJob(created);

    // Nothing inserted: another call already holds this (queue, key).
    const { rows } = await this.pool.query<PgRow>(
      `SELECT * FROM ${this.jobs} WHERE queue = $1 AND idempotency_key = $2`,
      [queue, idempotencyKey],
    );
    const existing = rows[0];
    if (!existing) throw new Error("insert was ignored but no job holds the idempotency key");
    if (canonicalize(existing.payload) !== canonicalize(JSON.parse(json))) {
      throw new IdempotencyConflictError(queue, idempotencyKey as string);
    }
    return toJob(existing);
  }

  async claim(queue: string, workerId: string): Promise<Job | null> {
    const now = this.now();

    // A job whose lease lapsed and that has used every attempt is not run again.
    // SKIP LOCKED: rows another claimer is touching are left for a later call
    // instead of waited on, so two claimers never block (or deadlock) each other.
    await this.pool.query(
      `UPDATE ${this.jobs}
          SET status = 'dead', finished_at = $1::bigint, lease_until = NULL,
              error = 'lease expired (worker ' || worker_id || ') after ' || attempts || ' attempts'
        WHERE id IN (
          SELECT id FROM ${this.jobs}
           WHERE queue = $2 AND status = 'running' AND lease_until <= $1::bigint
             AND attempts >= max_attempts
           FOR UPDATE SKIP LOCKED
        )`,
      [now, queue],
    );

    // FOR UPDATE SKIP LOCKED is the standard PostgreSQL job-queue pattern: each
    // claimer locks the first ready row nobody else has locked, so concurrent
    // workers get different jobs without waiting in line. As in SQLite, SET
    // expressions see the old row, so the CASE still reads the previous status.
    //
    // The attempts < max_attempts guard covers an exhausted job the statement
    // above skipped because it was locked: it must never be claimed again.
    const { rows } = await this.pool.query<PgRow>(
      `UPDATE ${this.jobs}
          SET error = CASE WHEN status = 'running'
                           THEN 'lease expired (worker ' || worker_id || ')'
                           ELSE error END,
              status = 'running', worker_id = $1, claimed_at = $2::bigint,
              attempts = attempts + 1, lease_until = $2::bigint + lease_ms
        WHERE id = (
          SELECT id FROM ${this.jobs}
           WHERE queue = $3
             AND ((status = 'pending' AND run_at <= $2::bigint)
               OR (status = 'running' AND lease_until <= $2::bigint AND attempts < max_attempts))
           ORDER BY run_at, seq
           LIMIT 1
           FOR UPDATE SKIP LOCKED
        )
       RETURNING *`,
      [workerId, now, queue],
    );
    const row = rows[0];
    return row ? toJob(row) : null;
  }

  async heartbeat(id: string, workerId: string): Promise<Job> {
    const { rows } = await this.pool.query<PgRow>(
      `UPDATE ${this.jobs} SET lease_until = $1::bigint + lease_ms
        WHERE id = $2 AND worker_id = $3 AND status = 'running'
       RETURNING *`,
      [this.now(), id, workerId],
    );
    const row = rows[0];
    if (!row) throw new JobStateError(`job ${id} is not running under worker ${workerId}`);
    return toJob(row);
  }

  async complete(id: string, workerId: string): Promise<void> {
    const { rowCount } = await this.pool.query(
      `UPDATE ${this.jobs} SET status = 'completed', finished_at = $1::bigint, lease_until = NULL
        WHERE id = $2 AND worker_id = $3 AND status = 'running'`,
      [this.now(), id, workerId],
    );
    if (rowCount === 0) throw new JobStateError(`job ${id} is not running under worker ${workerId}`);
  }

  async fail(id: string, workerId: string, error: string): Promise<Job> {
    // Read the attempt count, decide retry vs dead, then write. FOR UPDATE locks
    // the row until commit, so a claimer can't take the job in between.
    const row = await this.transaction(async (client) => {
      const held = await client.query<PgRow>(
        `SELECT * FROM ${this.jobs} WHERE id = $1 AND worker_id = $2 AND status = 'running' FOR UPDATE`,
        [id, workerId],
      );
      const job = held.rows[0];
      if (!job) throw new JobStateError(`job ${id} is not running under worker ${workerId}`);

      const now = this.now();
      const result =
        job.attempts >= job.max_attempts
          ? await client.query<PgRow>(
              `UPDATE ${this.jobs}
                  SET status = 'dead', error = $1, finished_at = $2::bigint, lease_until = NULL
                WHERE id = $3
               RETURNING *`,
              [error, now, id],
            )
          : await client.query<PgRow>(
              // Retrying releases the worker's hold so the job looks like any other pending job.
              `UPDATE ${this.jobs}
                  SET status = 'pending', error = $1, run_at = $2::bigint,
                      worker_id = NULL, claimed_at = NULL, lease_until = NULL
                WHERE id = $3
               RETURNING *`,
              [error, now + computeBackoff(job.attempts, this.retry, this.random), id],
            );
      return result.rows[0] as PgRow;
    });
    return toJob(row);
  }

  async listDead(queue: string, limit = 100): Promise<Job[]> {
    const { rows } = await this.pool.query<PgRow>(
      `SELECT * FROM ${this.jobs} WHERE queue = $1 AND status = 'dead' ORDER BY finished_at, seq LIMIT $2`,
      [queue, limit],
    );
    return rows.map(toJob);
  }

  async requeueDead(id: string): Promise<Job> {
    const { rows } = await this.pool.query<PgRow>(
      `UPDATE ${this.jobs}
          SET status = 'pending', attempts = 0, run_at = $1::bigint,
              worker_id = NULL, claimed_at = NULL, finished_at = NULL, lease_until = NULL
        WHERE id = $2 AND status = 'dead'
       RETURNING *`,
      [this.now(), id],
    );
    const row = rows[0];
    if (!row) throw new JobStateError(`job ${id} is not dead`);
    return toJob(row);
  }

  async get(id: string): Promise<Job | null> {
    const { rows } = await this.pool.query<PgRow>(`SELECT * FROM ${this.jobs} WHERE id = $1`, [id]);
    const row = rows[0];
    return row ? toJob(row) : null;
  }

  async close(): Promise<void> {
    await this.pool.end();
  }
}
