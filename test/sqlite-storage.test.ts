import { type ChildProcess, spawn } from "node:child_process";
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
    await store.complete(job.id, claimed!.leaseToken!);
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
    const claimed = await store.claim("q", "w1");
    expect((await store.fail(id, claimed!.leaseToken!, "x")).runAt).toBe(500); // lower bound of [500, 1000]
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

    expect(() => new SqliteStorage({ path })).toThrow(/schema version 0, expected \d+.*delete the file/);
  });
});

// #43: the build defaults a connection to an existing WAL file to synchronous =
// NORMAL, where a power loss can undo a commit that already returned (an
// enqueue the caller was told succeeded). Every connection must flush each
// commit, not only the one that happened to create the file.
describe("SqliteStorage durability", () => {
  const tempDirs: string[] = [];
  afterEach(() => {
    for (const dir of tempDirs.splice(0)) rmSync(dir, { recursive: true, force: true });
  });

  // The pragma is per connection, so it can only be read on the store's own
  // connection. Reaching into a private field is the price of testing it.
  const synchronous = (store: SqliteStorage) =>
    (store as unknown as { db: Database.Database }).db.pragma("synchronous", { simple: true });
  const FULL = 2;

  it("commits at synchronous = FULL on every connection, including to an existing file", async () => {
    const dir = mkdtempSync(join(tmpdir(), "jobq-durability-"));
    tempDirs.push(dir);
    const path = join(dir, "queue.sqlite");

    const creator = new SqliteStorage({ path });
    const otherProcess = new SqliteStorage({ path }); // the file is already in WAL mode now
    expect(synchronous(creator)).toBe(FULL);
    expect(synchronous(otherProcess)).toBe(FULL);
    await otherProcess.close();
    await creator.close();

    const afterRestart = new SqliteStorage({ path });
    expect(synchronous(afterRestart)).toBe(FULL);
    await afterRestart.close();
  });
});

// #45: SQLite's write lock has no queue. With busy_timeout, a waiting call slept
// inside SQLite (freezing the whole process) and could lose the lock to busier
// processes for seconds, then throw SQLITE_BUSY. The store now waits between
// attempts with the event loop free.
describe("SqliteStorage write-lock contention", () => {
  const tempDirs: string[] = [];
  const children: ChildProcess[] = [];
  afterEach(() => {
    for (const child of children.splice(0)) child.kill();
    for (const dir of tempDirs.splice(0)) rmSync(dir, { recursive: true, force: true });
  });
  const tempPath = (): string => {
    const dir = mkdtempSync(join(tmpdir(), "jobq-busy-"));
    tempDirs.push(dir);
    return join(dir, "queue.sqlite");
  };
  // Another process holding the write lock, as a second connection in this one.
  const holdLock = (path: string): Database.Database => {
    const other = new Database(path);
    other.exec("BEGIN IMMEDIATE");
    return other;
  };

  it("keeps the process responsive while waiting, and succeeds once the lock frees", async () => {
    const path = tempPath();
    const store = new SqliteStorage({ path });
    const other = holdLock(path);
    // Before #45 this timer could not fire: enqueue() blocked the process for
    // the whole busy_timeout, then threw SQLITE_BUSY.
    setTimeout(() => other.exec("COMMIT"), 200);

    const job = await store.enqueue({ queue: "q", payload: 1 });

    expect(job.status).toBe("pending");
    other.close();
    await store.close();
  });

  it("gives up with SQLITE_BUSY after busyTimeoutMs", async () => {
    const path = tempPath();
    const store = new SqliteStorage({ path, busyTimeoutMs: 100 });
    const other = holdLock(path);

    const started = performance.now();
    await expect(store.claim("q", "w1")).rejects.toMatchObject({ code: "SQLITE_BUSY" });
    expect(performance.now() - started).toBeGreaterThanOrEqual(100);

    other.exec("ROLLBACK");
    other.close();
    await store.close();
  });

  it("rejects a busyTimeoutMs that is negative or not an integer", () => {
    expect(() => new SqliteStorage({ path: ":memory:", busyTimeoutMs: -1 })).toThrow(RangeError);
    expect(() => new SqliteStorage({ path: ":memory:", busyTimeoutMs: 1.5 })).toThrow(RangeError);
  });

  // The scenario from #45: other processes write flat out. Before the fix,
  // calls here waited seconds with the process frozen, and some threw.
  it("doesn't fail or freeze while other processes write in a tight loop", async () => {
    const path = tempPath();
    const store = new SqliteStorage({ path });
    const hammer = join(import.meta.dirname, "helpers", "enqueue-hammer.ts");

    const outputs = Array.from({ length: 4 }, () => {
      const child = spawn(process.execPath, ["--import", "tsx", hammer, path], { stdio: ["pipe", "pipe", "inherit"] });
      children.push(child);
      let out = "";
      child.stdout!.on("data", (chunk) => { out += chunk; });
      const ready = new Promise<void>((resolve) => {
        const check = (): void => { if (out.includes("ready")) resolve(); else child.stdout!.once("data", check); };
        check();
      });
      const exited = new Promise<string>((resolve) => child.once("close", () => resolve(out)));
      return { child, ready, exited };
    });
    await Promise.all(outputs.map((o) => o.ready));

    // Measure the longest time the event loop went without running a 5 ms timer.
    let maxStallMs = 0;
    let last = performance.now();
    const ticker = setInterval(() => {
      const t = performance.now();
      maxStallMs = Math.max(maxStallMs, t - last);
      last = t;
    }, 5);

    let failures = 0;
    const end = performance.now() + 3_000;
    while (performance.now() < end) {
      try {
        const job = await store.enqueue({ queue: "q", payload: 1 });
        await store.complete(job.id, (await store.claim("q", "w1"))!.leaseToken!);
      } catch {
        failures++;
      }
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    clearInterval(ticker);

    for (const { child } of outputs) child.stdin!.end();
    const summaries = (await Promise.all(outputs.map((o) => o.exited)))
      .map((out) => JSON.parse(out.trim().split("\n").at(-1)!) as { written: number; failed: number });

    expect(failures).toBe(0);
    expect(summaries.every((s) => s.failed === 0)).toBe(true);
    // Each attempt fails at once rather than sleeping in SQLite, so the loop
    // only stalls for one write's own work. Generous for loaded CI machines;
    // before #45 single calls blocked for 1-5 s.
    expect(maxStallMs).toBeLessThan(250);
    await store.close();
  }, 30_000);
});
