import { execFile } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";
import { afterEach, expect, it } from "vitest";
import { SqliteStorage } from "../src/index.js";

const run = promisify(execFile);
const workerScript = join(import.meta.dirname, "helpers", "claim-worker.ts");

const tempDirs: string[] = [];
afterEach(() => {
  for (const dir of tempDirs.splice(0)) rmSync(dir, { recursive: true, force: true });
});

// fail() reads the attempt count and then writes. Several processes doing that
// at once must all succeed: a transaction that starts as a reader and then tries
// to write fails with SQLITE_BUSY_SNAPSHOT once another process has written.
it("lets competing processes fail different jobs at the same time", async () => {
  const dir = mkdtempSync(join(tmpdir(), "jobq-fail-race-"));
  tempDirs.push(dir);
  const dbPath = join(dir, "queue.sqlite");

  const JOBS = 300;
  const WORKERS = 4;

  const seed = new SqliteStorage({ path: dbPath });
  const enqueued = new Set<string>();
  for (let i = 0; i < JOBS; i++) {
    // maxAttempts 1: the first failure makes the job dead, so each job is failed exactly once.
    enqueued.add((await seed.enqueue({ queue: "q", payload: i, maxAttempts: 1 })).id);
  }
  await seed.close();

  const outputs = await Promise.all(
    Array.from({ length: WORKERS }, (_, i) =>
      run(process.execPath, ["--import", "tsx", workerScript, dbPath, `worker-${i}`, "fail"]),
    ),
  );
  const handled = outputs.flatMap((o) => JSON.parse(o.stdout.trim()) as string[]);

  expect(handled).toHaveLength(JOBS);
  expect(new Set(handled)).toEqual(enqueued);

  const check = new SqliteStorage({ path: dbPath });
  const dead = await check.listDead("q", JOBS + 1);
  expect(dead).toHaveLength(JOBS);
  for (const job of dead) expect(job).toMatchObject({ attempts: 1, error: "boom" });
  await check.close();
}, 60_000);
