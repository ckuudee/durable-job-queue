import { randomUUID } from "node:crypto";
import pg from "pg";
import { describe, expect, it } from "vitest";
import { PostgresStorage, type PostgresStorageOptions } from "../src/index.js";
import { storageContract } from "./storage-contract.js";

// These tests need a real PostgreSQL server, e.g. the one in docker-compose.yml:
//   JOBQ_PG_URL=postgres://jobq:jobq@localhost:5432/jobq
// Without it they are skipped, so `npm test` still works on a machine with no
// database. CI always sets it.
const url = process.env.JOBQ_PG_URL;

/** Every test gets its own schema, so tests can't see each other's jobs. */
function freshSchema(): string {
  return `jobq_test_${randomUUID().replaceAll("-", "")}`;
}

async function admin<T>(fn: (client: pg.Client) => Promise<T>): Promise<T> {
  const client = new pg.Client({ connectionString: url });
  await client.connect();
  try {
    return await fn(client);
  } finally {
    await client.end();
  }
}

async function dropSchema(schema: string): Promise<void> {
  await admin((c) => c.query(`DROP SCHEMA IF EXISTS ${schema} CASCADE`));
}

/** A store in a fresh schema that drops the schema when closed. */
async function isolatedStore(options: Omit<PostgresStorageOptions, "connectionString" | "schema">) {
  const schema = freshSchema();
  const store = await PostgresStorage.connect({ ...options, connectionString: url as string, schema });
  const close = store.close.bind(store);
  store.close = async () => {
    await close();
    await dropSchema(schema);
  };
  return store;
}

describe("PostgresStorage options", () => {
  // Validation happens before any connection is opened, so this needs no database.
  it("rejects a schema name that isn't a plain identifier", async () => {
    for (const schema of ["", "Jobs", "jobs; DROP TABLE x", "1jobs", "a".repeat(64)]) {
      await expect(
        PostgresStorage.connect({ connectionString: "postgres://unused", schema }),
      ).rejects.toThrow(RangeError);
    }
  });
});

describe.runIf(url)("PostgresStorage (needs JOBQ_PG_URL)", () => {
  storageContract("PostgreSQL", (options) => isolatedStore(options));

  it("lets many processes set up the same schema at once", async () => {
    const schema = freshSchema();
    try {
      const stores = await Promise.all(
        Array.from({ length: 5 }, () => PostgresStorage.connect({ connectionString: url as string, schema })),
      );
      await Promise.all(stores.map((s) => s.close()));
    } finally {
      await dropSchema(schema);
    }
  });

  it("refuses a schema written by a different schema version", async () => {
    const schema = freshSchema();
    try {
      await (await PostgresStorage.connect({ connectionString: url as string, schema })).close();
      await admin((c) => c.query(`UPDATE ${schema}.jobq_meta SET schema_version = 999`));
      await expect(PostgresStorage.connect({ connectionString: url as string, schema })).rejects.toThrow(
        /schema version 999, expected \d+.*drop the schema/,
      );
    } finally {
      await dropSchema(schema);
    }
  });

  it("round-trips payloads node-postgres would otherwise mangle", async () => {
    const store = await isolatedStore({});
    try {
      // A top-level array would become a PostgreSQL array literal if passed raw.
      for (const payload of [[1, "two", { three: 3 }], "text", 42, null, true]) {
        const job = await store.enqueue({ queue: "q", payload });
        expect((await store.get(job.id))?.payload).toEqual(payload);
      }
    } finally {
      await store.close();
    }
  });

  // The core guarantee, against a real server: workers with separate connection
  // pools race for the same jobs and each job is handed out exactly once.
  it("splits jobs across competing workers with no duplicates or losses", async () => {
    const schema = freshSchema();
    const JOBS = 300;
    const WORKERS = 4;
    const connect = () => PostgresStorage.connect({ connectionString: url as string, schema });
    try {
      const seed = await connect();
      const enqueued = new Set<string>();
      for (let i = 0; i < JOBS; i++) enqueued.add((await seed.enqueue({ queue: "q", payload: i })).id);

      const workers = await Promise.all(Array.from({ length: WORKERS }, connect));
      const handledByWorker = await Promise.all(
        workers.map(async (store, i) => {
          const handled: string[] = [];
          for (;;) {
            const job = await store.claim("q", `worker-${i}`);
            if (!job) return handled;
            handled.push(job.id);
            await store.complete(job.id, `worker-${i}`);
          }
        }),
      );
      const handled = handledByWorker.flat();

      expect(handled).toHaveLength(JOBS); // nothing lost, nothing claimed twice
      expect(new Set(handled)).toEqual(enqueued);
      for (const id of enqueued) {
        expect(await seed.get(id)).toMatchObject({ status: "completed", attempts: 1 });
      }
      await Promise.all([seed, ...workers].map((s) => s.close()));
    } finally {
      await dropSchema(schema);
    }
  }, 60_000);
});
