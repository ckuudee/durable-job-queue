// A live demo of the metrics dashboard: `npm run dashboard`, then open the URL
// it prints. Jobs keep arriving on email and reports; some fail and retry, a
// few end up dead, some are delayed. On a third queue, exports, a worker
// "crashes" holding a job every 15 s, so you can watch the job go stalled and
// then be taken over by another worker.
// Stop with Ctrl+C.
//
// Environment: PORT (default 4000), DEMO_DURATION_MS (stop after this long;
// used by the test that keeps this example working).
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as sleep } from "node:timers/promises";
import { SqliteStorage, Worker, startDashboard, type Job, type JobContext } from "../src/index.js";

const dir = mkdtempSync(join(tmpdir(), "jobq-dashboard-"));
const storage = new SqliteStorage({
  path: join(dir, "queue.sqlite"),
  retry: { baseDelayMs: 500, maxDelayMs: 5_000 },
  leaseMs: 4_000, // short, so a crashed worker's job is recovered within seconds
});

let stopping = false;
const dashboard = await startDashboard(storage, { port: Number(process.env.PORT ?? 4000) });
console.log(`Dashboard running at ${dashboard.url}  (Ctrl+C to stop)`);

// --- Producers ---------------------------------------------------------------

let n = 0;
const producer = setInterval(() => {
  n++;
  void storage.enqueue({ queue: "email", payload: { n, to: `user${n}@example.com` } });
  if (n % 3 === 0) void storage.enqueue({ queue: "reports", payload: { n }, maxAttempts: 2 });
  if (n % 5 === 0) void storage.enqueue({ queue: "email", payload: { n, digest: true }, delayMs: 8_000 });
}, 150);

// --- Workers -----------------------------------------------------------------

async function work(job: Job, { signal }: JobContext): Promise<void> {
  await sleep(50 + Math.random() * 400, undefined, { signal });
  // Some failures: they retry with backoff, and a job that fails every attempt goes dead.
  if (Math.random() < (job.queue === "reports" ? 0.3 : 0.08)) throw new Error("upstream returned 503");
}

// Crash recovery, on its own "exports" queue so it's easy to watch. Every 15 s
// an export job is claimed by a worker that then "crashes": it never heartbeats
// or records a result, exactly what a killed process leaves behind. The job
// shows as running until its 4 s lease lapses, then as stalled until exports-1
// (which checks only every 8 s) takes it over and finishes it.
async function simulateCrash(): Promise<void> {
  if (stopping) return;
  await storage.enqueue({ queue: "exports", payload: { at: Date.now() } });
  // SQLite answers without yielding to timers, and exports-1 sleeps between its
  // polls, so the crashed worker gets the job before exports-1 can look.
  const job = await storage.claim("exports", "crashed-worker");
  if (job) console.log(`crashed-worker took export job ${job.id.slice(0, 8)} and died`);
}
const crasher = setInterval(() => void simulateCrash(), 15_000);
await simulateCrash(); // awaited, so the first crash happens before exports-1 starts

const workers = [
  new Worker(storage, { queue: "email", handler: work, concurrency: 3, workerId: "email-1" }),
  new Worker(storage, { queue: "reports", handler: work, workerId: "reports-1" }),
  new Worker(storage, { queue: "exports", handler: work, workerId: "exports-1", pollIntervalMs: 8_000 }),
];
for (const worker of workers) worker.start();

// --- Shutdown ----------------------------------------------------------------

async function shutdown(): Promise<void> {
  if (stopping) return;
  stopping = true;
  clearInterval(producer);
  clearInterval(crasher);
  await Promise.all(workers.map((w) => w.stop({ timeoutMs: 2_000 })));
  await dashboard.close();
  await storage.close();
  rmSync(dir, { recursive: true, force: true });
  console.log("stopped");
}

process.once("SIGINT", () => void shutdown());
process.once("SIGTERM", () => void shutdown());
if (process.env.DEMO_DURATION_MS) {
  await sleep(Number(process.env.DEMO_DURATION_MS));
  await shutdown();
}
