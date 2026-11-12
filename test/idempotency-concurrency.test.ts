import { execFile } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";
import { afterEach, expect, it } from "vitest";
import { SqliteStorage } from "../src/index.js";

const run = promisify(execFile);
const workerScript = join(import.meta.dirname, "helpers", "enqueue-worker.ts");

const tempDirs: string[] = [];
afterEach(() => {
  for (const dir of tempDirs.splice(0)) rmSync(dir, { recursive: true, force: true });
});

// A producer that retries after a timeout, or several producers that all see the
// same event, must not create duplicate jobs. Separate OS processes race to
// enqueue the same keys; only the database can arbitrate between them.
it("creates exactly one job per key when separate processes enqueue the same keys", async () => {
  const dir = mkdtempSync(join(tmpdir(), "jobq-idem-"));
  tempDirs.push(dir);
  const dbPath = join(dir, "queue.sqlite");

  const KEYS = 100;
  const WORKERS = 4;

  // Open once up front so the schema exists before the processes race.
  await new SqliteStorage({ path: dbPath }).close();

  const outputs = await Promise.all(
    Array.from({ length: WORKERS }, (_, i) =>
      run(process.execPath, ["--import", "tsx", workerScript, dbPath, String(KEYS), String(i * 25)]),
    ),
  );
  const idsByWorker = outputs.map((o) => JSON.parse(o.stdout.trim()) as Record<string, string>);

  // Every process was told the same job id for every key.
  for (let n = 0; n < KEYS; n++) {
    const ids = new Set(idsByWorker.map((ids) => ids[`key-${n}`]));
    expect(ids.size).toBe(1);
  }

  // And the queue really holds one job per key, no more.
  const check = new SqliteStorage({ path: dbPath });
  let jobs = 0;
  while (await check.claim("q", "counter")) jobs++;
  await check.close();
  expect(jobs).toBe(KEYS);
}, 60_000);
