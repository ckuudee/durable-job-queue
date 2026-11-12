// A complete tour of the library in one runnable file: `npm run example`.
// Uses a throwaway SQLite file, so it needs no setup. To use PostgreSQL instead:
//   const storage = await PostgresStorage.connect({ connectionString: "postgres://..." });
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { JobStateError, Scheduler, SqliteStorage, type Job, type Storage } from "../src/index.js";

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

async function sendEmail(job: Job): Promise<void> {
  const { to, template } = job.payload as { to: string; template: string };
  await new Promise((resolve) => setTimeout(resolve, 20)); // pretend to call an email API
  if (to.endsWith("@invalid")) throw new Error(`mailbox ${to} does not exist`);
  log(`sent "${template}" to ${to} (attempt ${job.attempts})`);
}

/**
 * Claim one job and run it. Returns false when nothing is ready. This is the
 * whole worker protocol: claim, heartbeat while working, then complete or fail.
 */
async function workOnce(storage: Storage, queue: string, workerId: string): Promise<boolean> {
  const job = await storage.claim(queue, workerId);
  if (!job) return false;

  // Keep the lease alive for long jobs. If this worker crashed instead, the
  // lease would lapse and another worker would pick the job up.
  const heartbeat = setInterval(() => {
    storage.heartbeat(job.id, workerId).catch(() => {}); // a lost job surfaces in complete/fail
  }, job.leaseMs / 3);

  try {
    let failure: unknown;
    try {
      await sendEmail(job);
    } catch (err) {
      failure = err;
    }
    if (failure === undefined) {
      await storage.complete(job.id, workerId);
    } else {
      const after = await storage.fail(job.id, workerId, String(failure));
      log(after.status === "dead" ? `gave up on job: ${after.error}` : `failed, will retry at +${after.runAt - Date.now()} ms`);
    }
  } catch (err) {
    // Our lease lapsed and another worker took the job; it's theirs now.
    if (!(err instanceof JobStateError)) throw err;
    log(`lost job ${job.id} to another worker`);
  } finally {
    clearInterval(heartbeat);
  }
  return true;
}

// Two workers sharing the queue. Real workers loop forever; this demo stops after 2.5 s.
const stopAt = Date.now() + 2_500;
await Promise.all(
  ["worker-1", "worker-2"].map(async (workerId) => {
    while (Date.now() < stopAt) {
      const didWork = await workOnce(storage, "email", workerId);
      if (!didWork) await new Promise((resolve) => setTimeout(resolve, 50)); // idle: poll again shortly
    }
  }),
);

// --- Inspecting failures ------------------------------------------------------

await scheduler.stop();
const dead = await storage.listDead("email");
log(`dead-letter queue holds ${dead.length} job(s): ${dead.map((j) => j.error).join("; ")}`);
// After fixing the cause, put a dead job back: await storage.requeueDead(dead[0].id);

await storage.close();
rmSync(dir, { recursive: true, force: true });
