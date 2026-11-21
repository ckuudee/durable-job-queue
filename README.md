# durable-job-queue

A durable background job queue for Node.js: jobs survive crashes, failed jobs retry with backoff, and the system reports what it is doing.

> **Status:** working library with SQLite and PostgreSQL backends; every guarantee is tested in CI, PostgreSQL against a real server. Not yet published to npm. Load tests are next.

![The read-only dashboard: per-queue counts, throughput sparklines, and the dead-letter queue with each job's last error](docs/images/dashboard.png)

*The built-in dashboard during `npm run dashboard`: three queues under load, retries, and dead jobs. See [Monitor the queue](#monitor-the-queue).*

## Why

Most web apps need to do slow or unreliable work (send email, call a third-party API, process a file) outside the request. Doing that safely is harder than it looks: workers crash mid-job, two workers can grab the same job, and retries can run an action twice. This project is a from-scratch implementation that treats those failure cases as the main requirement.

## Features

- **Durable storage**: jobs persist in a database and are claimed atomically, and every claim gets a fencing token, so only the job's current holder can record its result. SQLite for a single machine, PostgreSQL (`FOR UPDATE SKIP LOCKED`) for many.
- **Workers** with configurable concurrency, automatic heartbeats, cancellation when a job is lost, and graceful shutdown.
- **Crash recovery**: a worker holds a job only for a lease it renews with heartbeats. If the worker dies, the lease lapses and another worker picks the job up.
- **Retries with exponential backoff and jitter**, and a **dead-letter queue** for jobs that keep failing.
- **Idempotency keys**, so retrying an enqueue after a timeout never creates a duplicate job.
- **Delayed jobs** (`delayMs` or `runAt`) and **recurring jobs** (cron expressions with time zones). Run a scheduler in every app instance: each occurrence still becomes exactly one job.
- **Monitoring**: a per-queue snapshot from the database (`stats()`), worker lifecycle events for your own metrics, and a read-only web dashboard.
- Planned: **load tests** with published numbers.

## Quick start

Requires Node.js 22 or later.

```powershell
git clone https://github.com/ckuudee/durable-job-queue.git
cd durable-job-queue
npm install
npm run example
```

[`examples/quickstart.ts`](examples/quickstart.ts) exercises every feature against a throwaway SQLite file and prints what happens:

```text
    +0ms  enqueued welcome email; the retried enqueue returned the same job: true
   +59ms  sent "welcome" to ada@example.com (attempt 1)
   +60ms  attempt 1 of 2 for nobody@invalid failed
  +151ms  sent "report" to ops@example.com (attempt 1)
  +152ms  attempt 2 of 2 for nobody@invalid failed
  +367ms  sent "tips" to ada@example.com (attempt 1)
 +1148ms  sent "report" to ops@example.com (attempt 1)
 +2180ms  sent "report" to ops@example.com (attempt 1)
 +2540ms  dead-letter queue holds 1 job(s): mailbox nobody@invalid does not exist
```

The snippets below are condensed from that file. CI runs the example on every change, so an API change that breaks it fails the build.

## Usage

### Pick a storage backend

```ts
import { PostgresStorage, SqliteStorage } from "durable-job-queue";

// One machine: an embedded SQLite file. Separate processes can share it.
const storage = new SqliteStorage({ path: "data/queue.sqlite" });

// Many machines: PostgreSQL. connect() creates the tables on first use.
const storage = await PostgresStorage.connect({ connectionString: process.env.DATABASE_URL! });
```

Both implement the same `Storage` interface and pass the same test suite, so the rest of this section works with either. Until the package is on npm, import from the cloned repo's `src/index.js` instead.

### Enqueue jobs

```ts
await storage.enqueue({ queue: "email", payload: { to: "ada@example.com", template: "welcome" } });

// Safe to retry: the same key returns the existing job instead of creating another.
await storage.enqueue({ queue: "email", payload, idempotencyKey: "welcome:ada" });

// Delayed: by a duration, or until a time.
await storage.enqueue({ queue: "email", payload, delayMs: 60_000 });
await storage.enqueue({ queue: "email", payload, runAt: new Date("2026-12-01T09:00:00Z") });

// Per-job options (defaults: 3 attempts, 30 s lease).
await storage.enqueue({ queue: "email", payload, maxAttempts: 5, leaseMs: 120_000 });
```

Payloads can be any JSON-serializable value, stored exactly as `JSON.stringify` writes it on both backends. Queue names, idempotency keys and worker ids must not contain U+0000 or unpaired surrogates (a `RangeError` says so); in a failure message, those characters are recorded as U+FFFD.

### Process jobs

```ts
import { Worker } from "durable-job-queue";

const worker = new Worker(storage, {
  queue: "email",
  concurrency: 5,                       // jobs at once in this process (default 1)
  handler: async (job, { signal }) => {
    await sendEmail(job.payload, { signal });   // throw to fail: retried with backoff, then dead
  },
});
worker.start();

// Graceful shutdown: stop claiming, let in-flight jobs finish (up to 30 s).
process.on("SIGTERM", () => worker.stop({ timeoutMs: 30_000 }));
```

The worker claims jobs, renews each job's lease with heartbeats while the handler runs, then completes or fails the job. `signal` is an `AbortSignal` that fires if the worker loses the job (its lease lapsed and another worker took it) or a shutdown times out. Long handlers should pass it to `fetch()` and similar calls: the result will not be recorded, and the job runs again elsewhere. Errors the worker survives, such as the database being briefly unreachable, go to an optional `onError` callback.

Under the hood a worker is a loop around four storage calls (`claim`, `heartbeat`, then `complete` or `fail`), which you can also call directly.

### Dead jobs

```ts
const dead = await storage.listDead("email");     // oldest failure first, with the last error
await storage.requeueDead(dead[0].id);             // after fixing the cause: a fresh set of attempts
```

### Recurring jobs

```ts
import { Scheduler } from "durable-job-queue";

const scheduler = new Scheduler(storage).add({
  name: "daily-report",              // stable: it's part of each occurrence's idempotency key
  cron: "0 9 * * *",                 // 5 fields, an optional leading seconds field, or "@daily"
  timezone: "America/New_York",      // default "UTC"
  queue: "reports",
  payload: { kind: "daily" },
});
scheduler.start();                   // ticks every 10 s
// on shutdown:
await scheduler.stop();
```

The scheduler stores nothing of its own. Each tick enqueues upcoming occurrences as delayed jobs with the idempotency key `cron:<name>:<time>`, so running several schedulers is safe. After downtime, the occurrence that was already queued runs late, and occurrences missed entirely are skipped rather than all fired at once.

Daylight saving time follows classic cron. When clocks spring forward, a time that doesn't exist that day (2:30 in New York) runs at the next valid time (3:30). When clocks fall back and an hour repeats, a job at a fixed time of day (`"30 1 * * *"`) runs once, at the first of the two 1:30s. A job that fires every hour or more often (`"30 * * * *"`) runs in both passes, because they are different real hours.

### Monitor the queue

The database's view, across every worker and process:

```ts
const [email] = await storage.stats("email");   // one row per queue; omit the name for all queues
// { queue: "email", ready: 12, scheduled: 3, running: 5, stalled: 0,
//   completed: 9_120, dead: 2, oldestReadyAgeMs: 850, at: 1_763_600_000_000 }
```

- `ready` and `oldestReadyAgeMs`: **is work backing up?** Age is measured from when a job became due, so delayed jobs and retries don't look late.
- `scheduled`: delayed jobs and retries waiting out their backoff.
- `stalled`: running jobs whose lease has lapsed. **The worker probably died**, and the next claim takes the job over.
- `dead`: **jobs that failed every attempt** (see `listDead` above).

One worker's view, as it happens, for feeding Prometheus, OpenTelemetry or logs:

```ts
worker.on("job:completed", ({ job, durationMs }) => metrics.observe(job.queue, durationMs));
worker.on("job:failed", ({ job, error, willRetry }) => log.warn({ id: job.id, error, willRetry }));
worker.on("job:abandoned", ({ job, reason }) => log.warn({ id: job.id, reason })); // lease lost, stop() timed out, or the result was rejected
```

Every claimed job gets exactly one of `job:completed`, `job:failed` or `job:abandoned`. A listener that throws can't break the worker: its error goes to `onError`. Events are per process and live in memory, so they can't see jobs that died inside another process. That's why the dashboard reads `stats()` instead.

#### Dashboard

```ts
import { startDashboard } from "durable-job-queue";

const dashboard = await startDashboard(storage, { port: 4000 });
console.log(`open ${dashboard.url}`);   // http://127.0.0.1:4000/
// on shutdown:
await dashboard.close();
```

A single page with no framework and no extra dependencies. It shows each queue's counts, throughput (completed jobs per second, as a sparkline), and the dead-letter queue with each job's last error. It refreshes every 2 seconds. To watch it with live traffic (jobs failing and retrying, delayed jobs, and a worker that "crashes" every 15 s so its job goes stalled and is taken over), run:

```powershell
npm run dashboard
```

The dashboard is **read-only and has no login**, so by default it only listens on this machine (`127.0.0.1`). It also rejects requests whose `Host` header isn't an expected name, which blocks [DNS rebinding](https://en.wikipedia.org/wiki/DNS_rebinding) (a malicious website reading it through a visitor's browser). Its Content Security Policy allows only its own inline script and style. Job payloads are never shown, since they may hold personal data. Before exposing it beyond localhost, put it behind a proxy that adds authentication, and set `host` and `allowedHosts`.

Design and tradeoffs: [docs/design/metrics.md](docs/design/metrics.md).

## Guarantees, and what this queue does not guarantee

- **At-least-once delivery, not exactly-once.** A worker can finish the work and crash before calling `complete`. The lease then lapses and the job runs again. Make handlers idempotent, for example by passing `job.idempotencyKey` or `job.id` to downstream APIs that support it.
- **One holder at a time per job.** Claiming is atomic, and each claim gets its own lease token (a fencing token). Once a job is re-claimed, the previous holder can no longer complete, fail or extend it, even if it has the same `workerId`. One `Worker` never runs the same job in two of its slots at once.
- **But a cut-off worker can briefly overlap.** If a worker loses contact with the database, its lease lapses and another worker may start the job while the first is still running it. The first worker's `signal` aborts as soon as its next heartbeat is rejected, and its result is never recorded. Handlers with side effects should honor the signal and be idempotent.
- **No migrations yet.** A database created by a different schema version is refused with a clear error instead of being upgraded.
- **Idempotency keys never expire**, because completed jobs are never deleted yet.

## Design goal

Correctness first. Each guarantee gets a test that tries to break it: separate OS processes racing for the same jobs, workers crashed mid-job, concurrent enqueues with the same key. Each feature is also checked by planting deliberate bugs and confirming the tests fail. A design doc covering the tradeoffs is on the roadmap.

## Stack

TypeScript, Node.js 22+, SQLite (better-sqlite3) and PostgreSQL (node-postgres) behind one storage interface, cron-parser for schedules, Vitest, GitHub Actions.

## Roadmap

- [x] Tooling and CI
- [x] Storage interface and SQLite backend
- [x] Enqueue, atomic claim, complete/fail
- [x] Worker with concurrency and graceful shutdown
- [x] Retries, backoff, dead-letter queue
- [x] Leases and crash recovery
- [x] Delayed jobs
- [x] Recurring jobs
- [x] Idempotency keys
- [x] PostgreSQL backend
- [x] Metrics and dashboard
- [ ] Load tests and design doc

## Running the tests

```powershell
npm test
```

The PostgreSQL backend runs the same contract tests as SQLite, plus a concurrency test, against a real server. They are skipped unless `JOBQ_PG_URL` is set; CI always runs them. Locally, with Docker running:

```powershell
docker compose up -d
$env:JOBQ_PG_URL = "postgres://jobq:jobq@127.0.0.1:5432/jobq"
npm test
```

## License

MIT
