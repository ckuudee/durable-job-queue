// A "target" is one throwaway database a single benchmark run uses: a fresh
// SQLite file in a temp directory, or a fresh schema in the PostgreSQL database
// named by JOBQ_PG_URL. It's plain JSON so it can be passed to child processes.
//
// Everything that reads results goes straight to the table with SQL rather than
// through the Storage API: the API has no "list every job" call (it doesn't need
// one), and reading timestamps from the rows means the library carries no
// benchmark-only code.
import { randomBytes } from "node:crypto";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import Database from "better-sqlite3";
import pg from "pg";
import { PostgresStorage, SqliteStorage, type Storage } from "../../src/index.js";

export type Backend = "sqlite" | "postgres";

export type Target =
  | { backend: "sqlite"; dir: string; path: string }
  | { backend: "postgres"; url: string; schema: string };

/** Every benchmark job goes in this queue. */
export const QUEUE = "bench";

/** Ids of rows inserted in bulk by seedCompleted(), so result queries can skip them. */
const SEED_PREFIX = "seed-";

export function pgUrl(): string {
  const url = process.env.JOBQ_PG_URL;
  if (!url) throw new Error("JOBQ_PG_URL is not set; it is needed for --backend postgres");
  return url;
}

/** Create an empty database (with the queue's tables) for one run. */
export async function createTarget(backend: Backend): Promise<Target> {
  if (backend === "sqlite") {
    const dir = mkdtempSync(join(tmpdir(), "jobq-bench-"));
    const target: Target = { backend, dir, path: join(dir, "queue.sqlite") };
    await (await openStorage(target)).close(); // creates the schema
    return target;
  }
  const target: Target = { backend, url: pgUrl(), schema: `bench_${randomBytes(6).toString("hex")}` };
  await (await openStorage(target)).close();
  return target;
}

export async function destroyTarget(target: Target): Promise<void> {
  if (target.backend === "sqlite") {
    rmSync(target.dir, { recursive: true, force: true });
    return;
  }
  await withPg(target, (c) => c.query(`DROP SCHEMA IF EXISTS ${target.schema} CASCADE`));
}

/** The library's own Storage for this target. `maxConnections` sizes the PostgreSQL pool. */
export async function openStorage(target: Target, maxConnections = 10): Promise<Storage> {
  if (target.backend === "sqlite") return new SqliteStorage({ path: target.path });
  return PostgresStorage.connect({ connectionString: target.url, schema: target.schema, maxConnections });
}

/** One job row, reduced to what the measurements need. Timestamps are epoch ms. */
export interface Row {
  id: string;
  status: string;
  attempts: number;
  createdAt: number;
  runAt: number;
  claimedAt: number | null;
  finishedAt: number | null;
  payload: unknown;
}

const ROW_COLUMNS = "id, status, attempts, created_at, run_at, claimed_at, finished_at, payload";

interface RawRow {
  id: string;
  status: string;
  attempts: number;
  created_at: number | string;
  run_at: number | string;
  claimed_at: number | string | null;
  finished_at: number | string | null;
  payload: string;
}

// pg returns BIGINT as a string (it can exceed 2^53); epoch ms never does.
const num = (v: number | string): number => Number(v);
const numOrNull = (v: number | string | null): number | null => (v === null ? null : Number(v));

function toRow(r: RawRow): Row {
  return {
    id: r.id,
    status: r.status,
    attempts: r.attempts,
    createdAt: num(r.created_at),
    runAt: num(r.run_at),
    claimedAt: numOrNull(r.claimed_at),
    finishedAt: numOrNull(r.finished_at),
    payload: JSON.parse(r.payload),
  };
}

/** Every job the run itself created (not the bulk-seeded ones). */
export async function readRows(target: Target): Promise<Row[]> {
  const where = `queue = '${QUEUE}' AND id NOT LIKE '${SEED_PREFIX}%'`;
  if (target.backend === "sqlite") {
    return withSqlite(target, (db) =>
      (db.prepare(`SELECT ${ROW_COLUMNS} FROM jobs WHERE ${where}`).all() as RawRow[]).map(toRow),
    );
  }
  return withPg(target, async (c) => {
    const { rows } = await c.query<RawRow>(`SELECT ${ROW_COLUMNS} FROM ${target.schema}.jobs WHERE ${where}`);
    return rows.map(toRow);
  });
}

/**
 * How many of the queue's jobs are still pending or running, for watching a run
 * progress. Deliberately not Storage.stats(): that counts every row in the
 * queue (about 300 ms per call on SQLite with a million completed rows), and a
 * SqliteStorage opens a write transaction on startup, so the harness would
 * compete with the workers it is measuring. This uses one read-only connection
 * and a count the claim index answers without touching completed rows.
 */
export class ProgressReader {
  private constructor(
    private readonly countFn: () => Promise<number>,
    private readonly closeFn: () => Promise<void>,
  ) {}

  static async open(target: Target): Promise<ProgressReader> {
    const sql = (jobs: string) =>
      `SELECT COUNT(*) AS n FROM ${jobs} WHERE queue = '${QUEUE}' AND status IN ('pending', 'running')`;
    if (target.backend === "sqlite") {
      const db = new Database(target.path, { readonly: true });
      const stmt = db.prepare(sql("jobs"));
      return new ProgressReader(
        async () => (stmt.get() as { n: number }).n,
        async () => void db.close(),
      );
    }
    const client = new pg.Client({ connectionString: target.url });
    await client.connect();
    return new ProgressReader(
      async () => Number((await client.query<{ n: string }>(sql(`${target.schema}.jobs`))).rows[0]!.n),
      () => client.end(),
    );
  }

  unfinished(): Promise<number> {
    return this.countFn();
  }

  close(): Promise<void> {
    return this.closeFn();
  }
}

/** Refresh the planner's statistics, as autovacuum would in a real deployment. */
export async function analyze(target: Target): Promise<void> {
  if (target.backend === "sqlite") withSqlite(target, (db) => db.exec("ANALYZE"));
  else await withPg(target, (c) => c.query(`ANALYZE ${target.schema}.jobs`));
}

/**
 * Insert `count` already-completed jobs in one SQL statement, to model a
 * long-lived queue whose finished jobs are never deleted. Their presence is all
 * that matters, so this skips enqueue/claim/complete. ANALYZE afterwards, as
 * autovacuum would in a real PostgreSQL deployment, so the planner knows the
 * table is large.
 */
export async function seedCompleted(target: Target, count: number): Promise<void> {
  if (count === 0) return;
  const at = Date.now() - 24 * 60 * 60 * 1000; // a day ago
  const columns =
    "id, queue, payload, status, attempts, max_attempts, created_at, run_at, lease_ms, claimed_at, finished_at";
  if (target.backend === "sqlite") {
    withSqlite(target, (db) => {
      db.prepare(
        `WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < ?)
         INSERT INTO jobs (${columns})
         SELECT '${SEED_PREFIX}' || i, ?, '{}', 'completed', 1, 3, ?, ?, 30000, ?, ? FROM n`,
      ).run(count, QUEUE, at, at, at, at);
      db.exec("ANALYZE");
    });
    return;
  }
  await withPg(target, async (c) => {
    await c.query(
      `INSERT INTO ${target.schema}.jobs (${columns})
       SELECT '${SEED_PREFIX}' || i, $1, '{}', 'completed', 1, 3, $2, $2, 30000, $2, $2
         FROM generate_series(1, $3::int) AS i`,
      [QUEUE, at, count],
    );
    await c.query(`ANALYZE ${target.schema}.jobs`);
  });
}

/**
 * The database's plan for finding the next job to claim. This repeats the
 * SELECT inside each backend's claim statement (src/sqlite-storage.ts and
 * src/postgres-storage.ts), so it must be kept in step with them.
 */
export async function explainClaim(target: Target): Promise<string> {
  const now = Date.now();
  if (target.backend === "sqlite") {
    return withSqlite(target, (db) =>
      (
        db
          .prepare(
            `EXPLAIN QUERY PLAN
             SELECT id FROM jobs
              WHERE queue = ?
                AND ((status = 'pending' AND run_at <= ?) OR (status = 'running' AND lease_until <= ?))
              ORDER BY run_at, seq
              LIMIT 1`,
          )
          .all(QUEUE, now, now) as { detail: string }[]
      )
        .map((r) => r.detail)
        .join("\n"),
    );
  }
  return withPg(target, async (c) => {
    const { rows } = await c.query<{ "QUERY PLAN": string }>(
      `EXPLAIN
       SELECT id FROM ${target.schema}.jobs
        WHERE queue = $1
          AND ((status = 'pending' AND run_at <= $2::bigint)
            OR (status = 'running' AND lease_until <= $2::bigint AND attempts < max_attempts))
        ORDER BY run_at, seq
        LIMIT 1
        FOR UPDATE SKIP LOCKED`,
      [QUEUE, now],
    );
    return rows.map((r) => r["QUERY PLAN"]).join("\n");
  });
}

function withSqlite<T>(target: Extract<Target, { backend: "sqlite" }>, fn: (db: Database.Database) => T): T {
  const db = new Database(target.path);
  try {
    return fn(db);
  } finally {
    db.close();
  }
}

async function withPg<T>(target: Extract<Target, { backend: "postgres" }>, fn: (c: pg.Client) => Promise<T>): Promise<T> {
  const client = new pg.Client({ connectionString: target.url });
  await client.connect();
  try {
    return await fn(client);
  } finally {
    await client.end();
  }
}

/**
 * The settings that decide what a number means, recorded with the results:
 * durability (does every commit reach the disk before returning?) and, for
 * PostgreSQL, the connection limit that rules out the largest layouts.
 */
export async function databaseSettings(backend: Backend): Promise<Record<string, string>> {
  if (backend === "sqlite") {
    // Pragmas are per connection, so read them on a store's own connection: the
    // store sets them in its constructor. Opened on an existing file, which is
    // the case every worker process is in.
    const target = await createTarget("sqlite");
    try {
      const store = (await openStorage(target)) as unknown as { db: Database.Database; close(): Promise<void> };
      const get = (name: string) => String(store.db.pragma(name, { simple: true }));
      const settings = {
        journal_mode: get("journal_mode"),
        synchronous: get("synchronous"),
        fullfsync: get("fullfsync"),
        busy_timeout: get("busy_timeout"),
      };
      await store.close();
      return settings;
    } finally {
      await destroyTarget(target);
    }
  }
  const client = new pg.Client({ connectionString: pgUrl() });
  await client.connect();
  try {
    const names = [
      "synchronous_commit",
      "fsync",
      "wal_sync_method",
      "full_page_writes",
      "max_connections",
      "superuser_reserved_connections",
      "shared_buffers",
    ];
    const settings: Record<string, string> = {};
    for (const name of names) settings[name] = (await client.query(`SHOW ${name}`)).rows[0][name];
    return settings;
  } finally {
    await client.end();
  }
}

/** Versions to record alongside results. */
export async function databaseVersions(backends: Backend[]): Promise<Record<string, string>> {
  const versions: Record<string, string> = {};
  const mem = new Database(":memory:");
  versions.sqlite = (mem.prepare("SELECT sqlite_version() AS v").get() as { v: string }).v;
  mem.close();
  if (backends.includes("postgres")) {
    const client = new pg.Client({ connectionString: pgUrl() });
    await client.connect();
    versions.postgres = (await client.query<{ version: string }>("SELECT version()")).rows[0]!.version;
    await client.end();
  }
  return versions;
}
