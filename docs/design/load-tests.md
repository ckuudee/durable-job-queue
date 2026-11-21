# Design: load tests

Status: **proposed.**

## Goal

Publish numbers that answer the questions someone asks before adopting a job queue, and show how they were measured so a reader can check them:

1. **How much work can it move?** Jobs per second when handlers do nothing, so the number is the queue's own overhead.
2. **How long does a job wait?** Time from enqueue to a worker picking it up, at a steady load below the maximum.
3. **Does it slow down as the table grows?** Completed jobs stay in the table (there is no pruning yet), so a long-lived queue has a large table.
4. **Do the guarantees hold under load?** Every run checks its own result: every job enqueued ran, and none ran twice.

Point 4 is what separates this from a plain benchmark. The test suite proves the guarantees with a few jobs and injected races; the load tests check that they still hold with tens of thousands of jobs and many processes competing for them.

## What gets measured, and where

All timings come from the job rows, not from the load generator. Every row already has `createdAt`, `runAt`, `claimedAt` and `finishedAt`, stamped by the store's clock (the database's `now()` for PostgreSQL, the shared system clock for SQLite). So:

- **Claim wait** = `claimedAt − runAt`: how long a ready job waited for a worker.
- **End to end** = `finishedAt − createdAt`: enqueue to completion as recorded.
- **Drain throughput** = jobs ÷ (`max(finishedAt)` − `min(claimedAt)`).

This needs no benchmark-only code in the library, and the numbers don't depend on when separate processes happened to report back. Timestamps are whole milliseconds, so very low latencies are reported as "≤ 1 ms" rather than with fake precision.

## Scenarios

Each scenario runs against both backends.

### S1: drain throughput

Preload 20,000 ready jobs, then start workers with a handler that returns immediately, and time the drain.

Matrix: **worker processes** {1, 2, 4, 8} × **concurrency per process** {1, 4, 16}.

**Why separate processes:** Node runs JavaScript on one thread, so several Workers in one process share it, and the test would measure Node instead of the database. Production deployments run many processes, so the runner spawns real child processes.

### S2: enqueue throughput

Producer processes {1, 4} enqueue 20,000 jobs as fast as they can, with and without an idempotency key, since the key adds a unique-index check.

### S3: latency at a steady rate

Enqueue at a fixed rate R for 60 seconds while 4 processes × 4 slots consume. R is 25%, 50% and 75% of the best S1 result for that worker layout, so the system is loaded but not saturated. Report claim wait and end to end at p50, p95, p99 and max.

**The load is open-loop:** the producer enqueues on a fixed timer, whatever the system is doing. A closed-loop generator (send the next job only once the last one is done) sends less work exactly when the system slows down, so the slow periods are undercounted and p99 looks better than it is. That error is called *coordinated omission*.

Run twice: with the default `pollIntervalMs` (1000) and with 10 ms. A Worker with nothing to do sleeps for the poll interval before claiming again, so at light load the default's latency mostly measures that setting. Reporting both shows the trade-off: shorter polls mean lower latency but more idle queries.

### S4: large tables

Repeat S1 (4 processes × 4 slots) with 0, 100,000 and 1,000,000 completed jobs already in the table. Setup inserts them in bulk with SQL, not through `enqueue`, because only their presence matters. Also capture the claim query's plan (`EXPLAIN QUERY PLAN` / `EXPLAIN`) to show it uses the claim index rather than scanning.

Expected result: throughput stays roughly flat, because claiming is an index lookup on `(queue, status, run_at, seq)`. If it doesn't, that's a finding worth publishing.

## Correctness check (every run)

After each run, read the table and fail the run, rather than report a number, if any of these is false:

- Completed jobs = jobs enqueued, and none is still pending, running or dead.
- Every job has `attempts = 1`. Leases are 30 s and no worker crashes, so a second attempt would mean two workers got the same job.
- No Worker emitted `job:abandoned`, and `onError` was never called.

## Method

- **Machine:** an Apple M2 MacBook (8 cores, 8 GB), plugged in, other apps closed. Node 26, native arm64. PostgreSQL 17 from Postgres.app (native arm64, not a Rosetta build). The results file records the machine, OS, Node, SQLite and PostgreSQL versions and the git commit.
- **Same machine:** the database, the producers and the workers share the same 8 cores. That makes the numbers conservative for PostgreSQL, which would normally run on its own host, and the write-up says so.
- **Default, durable settings:** PostgreSQL runs with its default configuration. SQLite uses the store's own settings (WAL mode, default `synchronous`). A queue that promises durability shouldn't be benchmarked with durability turned off. Each process's PostgreSQL pool is sized to its concurrency, so the pool isn't the bottleneck.
- **Repeats:** a fresh database for every run, one warm-up run discarded, then 3 runs. Report the median and the min–max range, not the best run.
- **Hypotheses written down first** (below), so the results can't be explained after the fact.

## Hypotheses

1. SQLite throughput peaks at 1–2 processes. SQLite allows one writer at a time, so more processes mostly wait for the write lock (`busy_timeout`) and may even lower throughput.
2. PostgreSQL throughput rises with processes until CPU runs out (around 4–8 processes on this machine, since the server shares the cores). `FOR UPDATE SKIP LOCKED` lets claimers take different rows instead of queuing for the same one.
3. With one process and one slot, SQLite beats PostgreSQL: an in-process library call costs less than a network round trip.
4. S4 throughput stays within 20% between an empty table and one with a million completed jobs.

## Deliverables

- `bench/`: the runner (`npm run bench`), with flags for backend, scenario and sizes. It is TypeScript run with `tsx` and needs no new dependencies.
- `bench/results/`: one JSON file per published run, committed, so the README's numbers can be traced to raw data.
- A "Performance" section in the README with a summary table, the machine, and a link to this doc.
- **CI smoke test:** the runner at a tiny size (a few hundred jobs) on both backends. It checks only that runs complete and pass the correctness check, and publishes no numbers, because shared CI runners are too noisy to compare.

## Non-goals

- **Comparisons with other queues** (BullMQ, pg-boss, etc.). A fair comparison needs matched durability settings and features; maybe later, as its own piece of work.
- **Crashes under load.** Crash recovery has its own tests. Killing workers mid-run would make the "attempts = 1" check meaningless, so it would need a different correctness check.
- **Tuning.** These are baseline numbers. If one result points at a bottleneck, fixing it is a separate PR with before and after numbers.

## Delivery plan (one PR each)

1. `docs: load test design` (this document)
2. `feat(bench): load test runner with SQLite results` (runner, S1–S4, correctness check, CI smoke job, SQLite results)
3. `docs: PostgreSQL load test results and README performance section`
