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

// The core guarantee: separate OS processes racing for the same jobs must each
// get distinct jobs, with none lost and none run twice.
it("splits jobs across competing processes with no duplicates or losses", async () => {
  const dir = mkdtempSync(join(tmpdir(), "jobq-race-"));
  tempDirs.push(dir);
  const dbPath = join(dir, "queue.sqlite");

  const JOBS = 300;
  const WORKERS = 4;

  const seed = new SqliteStorage({ path: dbPath });
  const enqueued = new Set<string>();
  for (let i = 0; i < JOBS; i++) enqueued.add((await seed.enqueue({ queue: "q", payload: i })).id);
  await seed.close();

  const outputs = await Promise.all(
    Array.from({ length: WORKERS }, (_, i) =>
      run(process.execPath, ["--import", "tsx", workerScript, dbPath, `worker-${i}`]),
    ),
  );
  const handledByWorker: string[][] = outputs.map((o) => JSON.parse(o.stdout.trim()));
  const handled = handledByWorker.flat();

  expect(handled).toHaveLength(JOBS); // nothing lost, nothing claimed twice
  expect(new Set(handled)).toEqual(enqueued); // exactly the jobs we enqueued

  const check = new SqliteStorage({ path: dbPath });
  for (const id of enqueued) {
    const job = await check.get(id);
    expect(job).toMatchObject({ status: "completed", attempts: 1 });
  }
  await check.close();
}, 60_000);
