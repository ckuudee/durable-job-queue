# Design: metrics and dashboard

Status: **accepted, not yet implemented.**

## Goal

Answer the questions an operator asks about a job queue:

1. **Is work backing up?** How many jobs are waiting, and how long has the oldest one waited?
2. **Is work failing?** How many jobs are retrying, and how many are in the dead-letter queue?
3. **Is anything stuck?** Are there running jobs whose worker has gone silent?
4. **How fast is it going?** Jobs completed per second, and how long handlers take.

## Two sources, two jobs

The design has two parts. Each gets its data from a different place, and that choice drives everything else.

| | `Storage.stats()` (pull) | Worker events (push) |
|---|---|---|
| Source | The database | One Worker process |
| Scope | Every worker, every process | Only the jobs this Worker ran |
| Survives restarts | Yes | No, lives in memory |
| Good for | The current state: depth, age, stuck jobs, dead jobs | Rates and timings, wiring into Prometheus, OpenTelemetry or logs |
| Used by the dashboard | Yes | No |

**Why the dashboard reads only the database:** in production, workers run in many processes on many machines, and the dashboard is yet another process. Events only exist inside the process that emitted them, so a dashboard built on events would see one worker's slice of the system. The database is the single place where the true state of every job lives, so `stats()` is the source of truth. Events are for users who want to feed their own monitoring stack.

## Part 1: `Storage.stats()`

### API

```ts
export interface QueueStats {
  queue: string;
  /** Pending and runAt has passed: waiting only for a free worker. */
  ready: number;
  /** Pending with runAt in the future: delayed jobs and retries waiting out their backoff. */
  scheduled: number;
  /** Running with a live lease. */
  running: number;
  /** Running, but the lease lapsed: the worker is probably dead and the next claim() will take the job. */
  stalled: number;
  completed: number;
  dead: number;
  /** now − runAt of the oldest ready job, or null when nothing is ready. The best single "are we keeping up" number. */
  oldestReadyAgeMs: number | null;
  /** The clock reading the snapshot was taken at, from the store's injected clock. */
  at: number;
}

interface Storage {
  // ...existing methods
  /** One snapshot per queue that has any jobs, or only `queue` when given. */
  stats(queue?: string): Promise<QueueStats[]>;
}
```

**Why split `pending` into `ready` and `scheduled`:** a queue holding 10,000 jobs delayed until tomorrow is healthy, and a queue with 10,000 jobs that are ready but not running is not. Both have 10,000 pending jobs, so one combined number would hide the difference.

**Why `stalled`:** a crashed worker leaves its jobs `running` until the lease lapses and another worker claims them. The stored status doesn't show this, but `lease_until < now` does. A growing `stalled` count means workers are dying, or there are no workers left to recover the jobs.

**Why `oldestReadyAgeMs` and not average wait:** the queue's worst-off job is what users complain about. It's also the standard way to alert on a queue ("oldest message age" in SQS, for example), and it takes one `MIN()` to compute.

### Query

One statement per backend, so the counts are a consistent snapshot. In SQLite a single statement reads one snapshot, and in PostgreSQL a single statement sees one MVCC snapshot. Running several separate `COUNT`s could double-count or miss a job that changes state between them.

```sql
-- SQLite version; PostgreSQL is the same with COUNT(*) FILTER (WHERE ...)
SELECT queue,
  SUM(status = 'pending' AND run_at <= :now)       AS ready,
  SUM(status = 'pending' AND run_at >  :now)       AS scheduled,
  SUM(status = 'running' AND lease_until >  :now)  AS running,
  SUM(status = 'running' AND lease_until <= :now)  AS stalled,  -- same boundary as claim()
  SUM(status = 'completed')                        AS completed,
  SUM(status = 'dead')                             AS dead,
  MIN(CASE WHEN status = 'pending' AND run_at <= :now THEN run_at END) AS oldest_ready_run_at
FROM jobs
WHERE (:queue IS NULL OR queue = :queue)
GROUP BY queue;
```

`:now` comes from the store's injected clock, never the database's `now()`. That matches the rule the backends already follow (`postgres-storage.ts` header comment), so the contract tests can control time.

### Cost and a known limitation

Both backends already have an index on `(queue, status, ...)`, so this query reads only the index and never touches the `jobs` table itself. It is still **O(number of jobs)**, and **completed jobs are never deleted today**, so the `completed` count grows forever. For a portfolio-scale queue this is fine. Before adding a cache or summary table I'd measure the cost in the load-test milestone. The real fix is a retention policy for completed jobs, which is a separate feature (see Decisions).

### Tests

Add the cases to `test/storage-contract.ts` so SQLite and PostgreSQL both have to pass them:

- One job in each bucket at a fixed clock reading gives the expected counts.
- Moving the clock past `runAt` turns `scheduled` into `ready`. Moving it past `leaseUntil` turns `running` into `stalled`.
- `oldestReadyAgeMs` is null for an empty queue and is computed from the oldest *ready* job, ignoring scheduled ones.
- `stats("a")` leaves out queue `b`, and `stats()` returns both.

## Part 2: Worker events

### API

`Worker` extends Node's `EventEmitter` with typed events:

```ts
interface WorkerEvents {
  "job:claimed":   [{ job: Job }];
  "job:completed": [{ job: Job; durationMs: number }];
  /** The handler threw. `willRetry` comes from the job fail() returned: pending means retry, dead means DLQ. */
  "job:failed":    [{ job: Job; error: unknown; durationMs: number; willRetry: boolean }];
  /** The worker lost the job (lease lapsed and it was re-claimed), stop() timed out, or storage rejected the result. No result was recorded. */
  "job:abandoned": [{ job: Job; reason: unknown }];
}
// Every job:claimed is followed by exactly one of the other three.

worker.on("job:completed", ({ job, durationMs }) => histogram.observe({ queue: job.queue }, durationMs));
```

**Why `EventEmitter`:** it's the standard Node pattern (`http.Server`, streams), it adds no dependency, and it doesn't tie the library to one metrics vendor. Users bridge to Prometheus or OpenTelemetry with a few lines. The alternative is a pluggable `Metrics` interface that the worker calls, which is what bigger libraries do. That's more API to design and document for no gain at this size.

### Two pitfalls the implementation must handle

1. **Don't name an event `"error"`.** `EventEmitter` throws if `"error"` is emitted with no listener, so emitting it would crash a worker that nobody is observing. Errors the worker survives keep going to the existing `onError` option.
2. **A listener that throws must not break the job.** `emit()` runs listeners synchronously inside `run()`. A bug in a listener could then turn a successful handler into a never-completed job. Wrap every emit in try/catch and pass the error to `onError`. This gets its own test.

### What events can't see

A job whose lease lapses on its *final* attempt is marked dead **inside another worker's `claim()`**, and nothing is returned, so no Worker emits an event for it. This is one more reason the dashboard uses `stats()`. It's documented as a limitation, not hidden.

### Tests (in `test/worker.test.ts`)

- A successful job emits `claimed` then `completed`, in that order, with `durationMs ≥ 0`.
- A failing job emits `failed` with `willRetry: true` until its last attempt, then `willRetry: false`.
- A heartbeat JobStateError (job lost) emits `abandoned` and no `completed`.
- A throwing listener is reported to `onError`, and the job still completes.

## Part 3: Dashboard

A small, read-only web page, with no frontend framework and no new dependencies.

- `startDashboard(storage, { port = 4000, host = "127.0.0.1" })` uses `node:http` and serves two routes:
  - `GET /api/stats` → `storage.stats()` as JSON.
  - `GET /` → one HTML page with inline CSS and JS that polls `/api/stats` every 2 s and shows a row per queue with the counts above.
- **Throughput without new storage:** the page computes completed jobs per second as `Δcompleted / Δt` between polls and draws the last few minutes as an inline SVG sparkline. This is correct only while completed jobs are never deleted, so it's tied to the retention decision below.
- The dead-letter list (via the existing `listDead`) shows the last error for each dead job.
- `npm run dashboard` starts an example that runs workers on a seeded mix of jobs (some fail, some are delayed), so there's something to watch. That's the demo for the README and interviews.

**Security:** the server binds to `127.0.0.1` by default, so it isn't reachable from the network. It is read-only in v1. A "requeue dead job" button would make it a state-changing endpoint, which would need CSRF protection and auth before being exposed anywhere, so it's deferred.

### Tests

- Start the server on port 0 (the OS picks a free port), call `/api/stats`, and check the JSON matches `storage.stats()`.
- `GET /` returns HTML with status 200.
- Unknown routes return 404, and non-GET methods return 405.

## Delivery plan (one PR each)

1. `feat: Storage.stats() queue snapshot` (interface, both backends, contract tests)
2. `feat: Worker lifecycle events` (events, the listener-safety wrapper, tests)
3. `feat: read-only metrics dashboard` (server, page, example, tests)
4. `docs: metrics in README` (plus interview notes)

## Decisions

1. **Retention of completed jobs comes after this work.** Metrics ship first. When `pruneCompleted(olderThanMs)` is added, the `completed` count and the dashboard's throughput calculation must change with it, because pruning makes `Δcompleted` go negative. The likely fix is a `completedSince` count backed by an index on `finished_at`.
2. **`stats()` returns an array of per-queue rows.** Rows map one-to-one onto the `GROUP BY` result and the dashboard's table, and an array keeps a stable order.
3. **The lost-job event is named `job:abandoned`.** It covers a lapsed lease, a `stop()` timeout and a result storage rejected, so the name describes what the worker did (gave up the job) rather than one cause.
4. **No `recovered` flag on `job:claimed`** (dropped during implementation). The only way to tell from a `Job` that the previous holder's lease lapsed is to match its `error` text against "lease expired". That text survives `requeueDead()`, and a handler can throw an error that says the same thing, so the flag would sometimes be wrong. Adding it properly means a dedicated column; until a metric needs it, `stalled` in `stats()` covers the operator's question.
