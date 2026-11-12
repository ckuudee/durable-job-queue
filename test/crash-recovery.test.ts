import { execFile } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as sleep } from "node:timers/promises";
import { promisify } from "node:util";
import { afterEach, expect, it } from "vitest";
import { SqliteStorage, type Job } from "../src/index.js";

const run = promisify(execFile);
const crashScript = join(import.meta.dirname, "helpers", "crash-worker.ts");

const tempDirs: string[] = [];
afterEach(() => {
  for (const dir of tempDirs.splice(0)) rmSync(dir, { recursive: true, force: true });
});

// The scenario leases exist for: a real worker process claims a job and is then
// killed mid-job. The job must be stuck while the lease is live, then be handed
// to a surviving worker once it lapses. Uses real time, so the lease is short.
it("recovers a job whose worker process was killed mid-job", async () => {
  const dir = mkdtempSync(join(tmpdir(), "jobq-crash-"));
  tempDirs.push(dir);
  const dbPath = join(dir, "queue.sqlite");

  const store = new SqliteStorage({ path: dbPath });
  const { id } = await store.enqueue({ queue: "q", payload: "important", leaseMs: 1_500 });

  // The child is killed by SIGKILL, so the call rejects; that is the point.
  await run(process.execPath, ["--import", "tsx", crashScript, dbPath]).catch(() => undefined);

  expect(await store.get(id)).toMatchObject({ status: "running", workerId: "crashed-worker", attempts: 1 });
  expect(await store.claim("q", "survivor")).toBeNull(); // lease still live: job is not up for grabs yet

  let recovered: Job | null = null;
  for (const deadline = Date.now() + 10_000; !recovered && Date.now() < deadline; ) {
    recovered = await store.claim("q", "survivor");
    if (!recovered) await sleep(100);
  }

  expect(recovered).toMatchObject({ id, workerId: "survivor", attempts: 2, payload: "important" });
  expect(recovered?.error).toMatch(/lease expired \(worker crashed-worker\)/);

  await store.complete(id, "survivor");
  expect(await store.get(id)).toMatchObject({ status: "completed", workerId: "survivor" });
  await store.close();
}, 30_000);
