// Opens a fresh, isolated store for either backend so every adversarial test
// can run against SQLite and PostgreSQL with the same code (guarantee S19).
import { randomUUID } from "node:crypto";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import pg from "pg";
import type { RetryPolicy } from "../../../src/backoff.js";
import { PostgresStorage, SqliteStorage, type Storage } from "../../../src/index.js";

export const PG_URL = process.env.JOBQ_PG_URL;

export interface StoreOptions {
  now?: () => number;
  retry?: Partial<RetryPolicy>;
  random?: () => number;
  leaseMs?: number;
}

/** Where a store lives, so a child process can open the same one. */
export type Location =
  | { kind: "sqlite"; path: string }
  | { kind: "postgres"; url: string; schema: string };

export interface Opened {
  storage: Storage;
  location: Location;
  /** Another store on the same database (e.g. a second "machine" with its own clock). */
  reopen(options?: StoreOptions): Promise<Storage>;
  cleanup(): Promise<void>;
}

export interface Backend {
  name: "sqlite" | "postgres";
  open(options?: StoreOptions): Promise<Opened>;
}

export async function openLocation(location: Location, options: StoreOptions = {}): Promise<Storage> {
  return location.kind === "sqlite"
    ? new SqliteStorage({ path: location.path, ...options })
    : PostgresStorage.connect({ connectionString: location.url, schema: location.schema, ...options });
}

const sqlite: Backend = {
  name: "sqlite",
  async open(options = {}) {
    const dir = mkdtempSync(join(tmpdir(), "jobq-adv-"));
    const location: Location = { kind: "sqlite", path: join(dir, "queue.sqlite") };
    const stores: Storage[] = [];
    const reopen = async (o: StoreOptions = {}): Promise<Storage> => {
      const s = await openLocation(location, o);
      stores.push(s);
      return s;
    };
    const storage = await reopen(options);
    return {
      storage,
      location,
      reopen,
      async cleanup() {
        for (const s of stores) await s.close().catch(() => {});
        rmSync(dir, { recursive: true, force: true });
      },
    };
  },
};

const postgres: Backend = {
  name: "postgres",
  async open(options = {}) {
    const url = PG_URL as string;
    const schema = `adv_${randomUUID().replaceAll("-", "").slice(0, 16)}`;
    const location: Location = { kind: "postgres", url, schema };
    const stores: Storage[] = [];
    const reopen = async (o: StoreOptions = {}): Promise<Storage> => {
      const s = await openLocation(location, o);
      stores.push(s);
      return s;
    };
    const storage = await reopen(options);
    return {
      storage,
      location,
      reopen,
      async cleanup() {
        for (const s of stores) await s.close().catch(() => {});
        const client = new pg.Client({ connectionString: url });
        await client.connect();
        try {
          await client.query(`DROP SCHEMA IF EXISTS ${schema} CASCADE`);
        } finally {
          await client.end();
        }
      },
    };
  },
};

/** SQLite always; PostgreSQL only when JOBQ_PG_URL is set (CI always sets it). */
export const BACKENDS: Backend[] = PG_URL ? [sqlite, postgres] : [sqlite];

/** A clock the test moves by hand. */
export class FakeClock {
  constructor(public t = 1_700_000_000_000) {}
  now = (): number => this.t;
  advance(ms: number): void {
    this.t += ms;
  }
}

export const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

/** Poll until `check` is true or fail after `timeoutMs`. */
export async function waitFor(check: () => boolean | Promise<boolean>, timeoutMs = 5_000, what = "condition"): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!(await check())) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);
    await sleep(10);
  }
}
