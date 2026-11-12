import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import Database from "better-sqlite3";
import { afterEach, describe, expect, it } from "vitest";
import { SqliteStorage } from "../src/index.js";
import { storageContract } from "./storage-contract.js";

storageContract(
  "SQLite (in-memory)",
  async ({ now, retry, leaseMs }) => new SqliteStorage({ path: ":memory:", now, retry, leaseMs }),
);

describe("SqliteStorage clock", () => {
  it("stamps jobs using the injected clock", async () => {
    let t = 1_000;
    const store = new SqliteStorage({ path: ":memory:", now: () => t });

    const job = await store.enqueue({ queue: "q", payload: 1 });
    expect(job.createdAt).toBe(1_000);

    t = 2_500;
    const claimed = await store.claim("q", "w1");
    expect(claimed?.claimedAt).toBe(2_500);

    t = 4_000;
    await store.complete(job.id, "w1");
    expect((await store.get(job.id))?.finishedAt).toBe(4_000);
    await store.close();
  });
});

describe("SqliteStorage options", () => {
  it("applies jitter from the injected random source", async () => {
    const store = new SqliteStorage({
      path: ":memory:",
      now: () => 0,
      retry: { baseDelayMs: 1_000, jitter: true },
      random: () => 0,
    });
    const { id } = await store.enqueue({ queue: "q", payload: 1 });
    await store.claim("q", "w1");
    expect((await store.fail(id, "w1", "x")).runAt).toBe(500); // lower bound of [500, 1000]
    await store.close();
  });

  it("rejects an invalid retry policy or lease length at construction", () => {
    expect(() => new SqliteStorage({ path: ":memory:", retry: { factor: 0.5 } })).toThrow(RangeError);
    expect(() => new SqliteStorage({ path: ":memory:", leaseMs: 0 })).toThrow(RangeError);
  });

  it("defaults to a 30 second lease", async () => {
    const store = new SqliteStorage({ path: ":memory:", now: () => 0 });
    await store.enqueue({ queue: "q", payload: 1 });
    expect((await store.claim("q", "w1"))?.leaseUntil).toBe(30_000);
    await store.close();
  });
});

describe("SqliteStorage schema version", () => {
  const tempDirs: string[] = [];
  afterEach(() => {
    for (const dir of tempDirs.splice(0)) rmSync(dir, { recursive: true, force: true });
  });

  function tempPath(): string {
    const dir = mkdtempSync(join(tmpdir(), "jobq-schema-"));
    tempDirs.push(dir);
    return join(dir, "queue.sqlite");
  }

  it("reopens a database it created", async () => {
    const path = tempPath();
    const first = new SqliteStorage({ path });
    const { id } = await first.enqueue({ queue: "q", payload: "kept" });
    await first.close();

    const second = new SqliteStorage({ path });
    expect(await second.get(id)).toMatchObject({ payload: "kept" });
    await second.close();
  });

  it("rejects a database file from an older schema with a clear message", () => {
    const path = tempPath();
    const old = new Database(path);
    old.exec("CREATE TABLE jobs (id TEXT PRIMARY KEY, status TEXT)"); // pre-versioning layout
    old.close();

    expect(() => new SqliteStorage({ path })).toThrow(/schema version 0, expected 3.*delete the file/);
  });
});
