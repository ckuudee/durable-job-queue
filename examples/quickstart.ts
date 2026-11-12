// A complete tour of the library in one runnable file: `npm run example`.
// Uses a throwaway SQLite file, so it needs no setup. To use PostgreSQL instead:
//   const storage = await PostgresStorage.connect({ connectionString: "postgres://..." });
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout } from "node:timers/promises";
import { Scheduler, SqliteStorage, Worker, type Job, type JobContext } from "../src/index.js";

const dir = mkdtempSync(join(tmpdir(), "jobq-example-"));
const storage = new SqliteStorage({
  path: join(dir, "queue.sqlite"),
  retry: { baseDelayMs: 100 }, // short so the demo finishes quickly; the default is 1 s
});
const started = Date.now();
const log = (msg: string) => console.log(`${`+${Date.now() - started}ms`.padStart(8)}  ${msg}`);

// --- Producing jobs ----------------------------------------------------------

// Idempotency key: retrying this call (say, after a network timeout) returns the
// same job instead of sending a second welcome email.
const welcome = await storage.enqueue({
  queue: "email",
  payload: { to: "ada@example.com", template: "welcome" },
  idempotencyKey: "welcome:ada",
});
const retried = await storage.enqueue({
  queue: "email",
  payload: { to: "ada@example.com", template: "welcome" },
  idempotencyKey: "welcome:ada",
});
log(`enqueued welcome email; the retried enqueue returned the same job: ${retried.id === welcome.id}`);

// Delayed job: not handed to a worker until 300 ms from now.
await storage.enqueue({ queue: "email", payload: { to: "ada@example.com", template: "tips" }, delayMs: 300 });

// A job that will always fail, to show retries and the dead-letter queue.
await storage.enqueue({ queue: "email", payload: { to: "nobody@invalid", template: "welcome" }, maxAttempts: 2 });

// Recurring job: a cron expression (here with a seconds field: every second).
// Run a scheduler in every app instance if you like; each occurrence still
// becomes exactly one job.
const scheduler = new Scheduler(storage, { lookaheadMs: 1_000 }).add({
  name: "heartbeat-report",
  cron: "* * * * * *",
  queue: "email",
  payload: { to: "ops@example.com", template: "report" },
});
scheduler.start(200);

// --- Processing jobs ---------------------------------------------------------

// The handler: resolve to complete the job, throw to fail it (it then retries
// with backoff, or goes to the dead-letter queue after maxAttempts). The worker
// claims jobs, keeps their leases alive with heartbeats, and records the result.
async function sendEmail(job: Job, { signal }: JobContext): Promise<void> {
  const { to, template } = job.payload as { to: string; template: string };
  // Pretend to call an email API. Passing `signal` on lets the call be cancelled
  // if this worker loses the job or is shut down mid-send.
  await setTimeout(20, undefined, { signal });
  if (to.endsWith("@invalid")) {
    log(`attempt ${job.attempts} of ${job.maxAttempts} for ${to} failed`);
    throw new Error(`mailbox ${to} does not exist`);
  }
  log(`sent "${template}" to ${to} (attempt ${job.attempts})`);
}

// Two workers sharing the queue, as if in two processes. Each could also run
// several jobs at once with `concurrency`.
const workers = ["worker-1", "worker-2"].map(
  (workerId) => new Worker(storage, { queue: "email", handler: sendEmail, workerId, pollIntervalMs: 50 }),
);
for (const worker of workers) worker.start();

// Real workers run until the process is told to stop, e.g.
//   process.on("SIGTERM", () => worker.stop({ timeoutMs: 30_000 }));
// This demo stops after 2.5 s. stop() lets in-flight jobs finish first.
await setTimeout(2_500);
await Promise.all([scheduler.stop(), ...workers.map((worker) => worker.stop())]);

// --- Inspecting failures ------------------------------------------------------

const dead = await storage.listDead("email");
log(`dead-letter queue holds ${dead.length} job(s): ${dead.map((j) => j.error).join("; ")}`);
// After fixing the cause, put a dead job back: await storage.requeueDead(dead[0].id);

await storage.close();
rmSync(dir, { recursive: true, force: true });
