# Design: load tests

Status: **accepted; revised after an independent review of the runner (PR #42).** The changes are listed under [Revisions](#revisions-after-the-review-of-the-runner).

## Goal

Publish numbers that answer the questions someone asks before adopting a job queue, and show how they were measured so a reader can check them:

1. **How much work can it move?** Jobs per second when handlers do nothing, so the number is the queue's own overhead.
2. **How long does a job wait?** Time from enqueue to a worker picking it up, at a steady load below the maximum.
3. **Does it slow down as the table grows?** Completed jobs stay in the table (there is no pruning yet), so a long-lived queue has a large table. And a queue that falls behind has many *pending* jobs, which turns out to matter more (S5).
4. **Do the guarantees hold under load?** Every run checks its own result: every job enqueued ran, and none ran twice.

Point 4 is what separates this from a plain benchmark. The test suite proves the guarantees with a few jobs and injected races; the load tests check that they still hold with tens of thousands of jobs and many processes competing for them.

## What gets measured, and where

All timings come from the job rows, not from the load generator. Every row already has `createdAt`, `runAt`, `claimedAt` and `finishedAt`. Both stores stamp them with the client process's `Date.now()` (the PostgreSQL store doesn't use the database's `now()`); every process runs on one machine, so they share one clock. So:

- **Claim wait** = `claimedAt − runAt`: how long a job waited for a worker after its row was written. The two backends stamp `claimedAt` at slightly different points. SQLite stamps it inside the write lock, so the wait includes waiting for that lock. PostgreSQL stamps it before borrowing a pooled connection and before the claim's first round trip, so the wait excludes those. Compare claim waits *within* a backend, not across them.
- **End to end** = `finishedAt` − the producer's *intended* send time (carried in the payload): the whole trip, including any delay in the producer itself (see S3).
- **Drain throughput:** from completion times; see S1 and S5.

This needs no benchmark-only code in the library, and the numbers don't depend on when separate processes happened to report back. Timestamps are whole milliseconds, so very low latencies are reported as "≤1 ms" rather than with fake precision.

## Scenarios

Each scenario runs against both backends.

### S1: drain throughput

Preload 3,000 ready jobs, then start workers with a handler that returns immediately. The headline number is the throughput **while the pending backlog fell from 2,000 to 1,000**, read from the completion times. The whole-drain rate is reported next to it.

Matrix: **worker processes** {1, 2, 4, 8} × **concurrency per process** {1, 4, 16}. On PostgreSQL, 8 × 16 needs 136 connections and the default `max_connections` is 100, so that cell is reported as "not run" with the reason. The configuration stays at its defaults on purpose. (In production you'd put a pooler such as PgBouncer in front, or raise the limit; that's an operator note, not a benchmark knob.)

**Why a fixed backlog:** the original plan timed a whole 20,000-job drain. The review showed that this mostly measures the preload size: each claim costs more the more jobs are pending (S5), so a bigger preload gives a lower number. Measuring every layout at the same backlog makes the layouts comparable.

**Why separate processes:** Node runs JavaScript on one thread, so several Workers in one process share it, and the test would measure Node instead of the database. Production deployments run many processes, so the runner spawns real child processes. They report "ready" once connected (with their connection pools already open), and all start on one "go" message.

### S2: enqueue throughput

Producer processes {1, 4} enqueue 20,000 jobs between them, each awaiting one call before the next, with and without an idempotency key (the key adds a unique-index check). Reports jobs/s and the duration of each `enqueue()` call.

### S3: latency at a steady rate

Enqueue at a fixed rate R for 60 seconds while 4 processes × 4 slots consume. R is 25%, 50% and 75% of **S1's low-backlog number for 4 × 4**. Report claim wait and end to end at p50, p95 and p99 (median of runs, with the p99 range) and the worst max of any run. Also record the most jobs pending or running at once: if that keeps growing, the load wasn't really below capacity.

**The load is open-loop:** the producer enqueues on a fixed timer, whatever the system is doing. A closed-loop generator (send the next job only once the last one is done) sends less work exactly when the system slows down, so the slow periods are undercounted and p99 looks better than it is. That error is called *coordinated omission*. End-to-end latency is measured from each job's *intended* send time, so a producer that falls behind (blocked on SQLite's write lock, say) counts against the queue. Claim wait starts at the row's write, so it doesn't include that delay.

Run twice: with the default `pollIntervalMs` (1000) and with 10 ms. A Worker with nothing to do sleeps for the poll interval before claiming again, so at light load the default's latency mostly measures that setting. Reporting both shows the trade-off: shorter polls mean lower latency but more idle queries.

### S4: completed rows

Repeat S1 (4 processes × 4 slots, low-backlog number) with 0, 100,000 and 1,000,000 completed jobs already in the table. Setup inserts them in bulk with SQL, not through `enqueue`, because only their presence matters. Capture the claim query's plan (`EXPLAIN QUERY PLAN` / `EXPLAIN`) after `ANALYZE`, with the pending jobs in place.

**What S4 can and can't show:** the claim query never touches completed rows, so flat throughput here is close to guaranteed by construction. What it does check is that the claim index keeps doing its job as the table grows. It can't show the effect of *pending* jobs; S5 does.

### S5: throughput against pending backlog

One large drain with 4 × 4, read from its completion times. Claims follow `ORDER BY run_at, seq` and at most (processes × slots) jobs are in flight, so when the k-th job completes, about N − k are still pending. Sort the rows by `finishedAt`, take windows of up to 1,000 completions, and report jobs/s against the pending backlog at each window. Windows near the end, where fewer jobs remain than 10× the slot count, are dropped: slots go idle there.

Sizes step down, 60,000 → 20,000 → 5,000, until one size passes every run. See [When a run fails](#when-a-run-fails).

## Correctness check (every run)

After each run, read the table and fail the run, rather than report a number, if any of these is false:

- Completed jobs = jobs enqueued, and none is still pending, running or dead.
- Every job has `attempts = 1`. Leases are 30 s and no worker crashes, so a second attempt means a job was claimed twice.
- Every job's handler ran **exactly once**. The bench handler records each job id it runs, and the runner compares those ids with the rows. The database alone can only show a job was *claimed* once; a Worker that ran a handler twice, or skipped it, would pass a database-only check.
- No Worker emitted `job:abandoned`, and `onError` was never called.

## When a run fails

- **A broken guarantee in the S5 sweep** is recorded as "fails at this size" with a pass count ("failed 1 of 4 runs"), never as the median of the runs that passed: that would hide how often it fails (survivorship bias). The row carries facts, not a throughput number: jobs claimed more than once, handlers not run exactly once, abandoned jobs, worker errors and the first error message. A failing run contributes no curve, because a job that ran twice carries its second attempt's timestamps. The sweep then moves on to the next smaller size.
- **A broken guarantee anywhere else** (S1–S4) stops the runner.
- **A harness failure** (a crashed child, a stall, an impossible number such as NaN) always stops the runner. Otherwise a benchmark bug would be published as a finding about the queue.
- The results file is written first, then the runner exits with code 1, so the data survives and CI goes red on any broken guarantee.
- **Interrupted runs** (Ctrl-C, a cancelled CI job): the runner kills its workers and deletes the run's database before exiting. Workers also exit by themselves if the runner disappears.

## Method

- **Machine:** an Apple M2 MacBook (8 cores, 8 GB), plugged in, other apps closed. Node 26, native arm64. PostgreSQL 19 from Postgres.app (native arm64, not a Rosetta build). CI tests against PostgreSQL 17, so before measuring, the full Postgres test suite is run against the 19 server too. The results file records the machine, OS, Node, SQLite and PostgreSQL versions, the git commit, and each database's durability and connection settings.
- **Same machine:** the database, the producers and the workers share the same 8 cores. That makes the numbers conservative for PostgreSQL, which would normally run on its own host, and the write-up says so.
- **Durable settings on both sides:** every commit is flushed before it returns. PostgreSQL runs its default configuration (`synchronous_commit = on`). SQLite runs at `synchronous = FULL`, which the store now sets on every connection (#43). Before that fix, most connections ran at NORMAL, which would have flattered SQLite in a head-to-head. On macOS, neither backend's default forces the drive's own write cache to disk (`F_FULLFSYNC`); both are treated the same.
- **Pools:** each worker process's PostgreSQL pool has one connection per slot plus one, opened before the timed run starts.
- **Repeats:** a fresh database for every run, one warm-up run discarded, then 3 runs. Report the median and the min–max range, not the best run. Each run starts fresh processes, so the warm-up run warms the database and the OS caches, not the measured processes' JIT.
- **The harness stays out of the way:** progress is watched with a read-only count of pending and running jobs, which the claim index answers. The original check used `stats()`, which counts every row (about 300 ms per call on SQLite with a million rows) and took SQLite's write lock.
- **Hypotheses written down first** (below), so the results can't be explained after the fact.

## Hypotheses

1. SQLite throughput peaks at 1–2 processes. SQLite allows one writer at a time, so more processes mostly wait for the write lock (`busy_timeout`) and may even lower throughput.
2. PostgreSQL throughput rises with processes until CPU runs out (around 4–8 processes on this machine, since the server shares the cores). `FOR UPDATE SKIP LOCKED` lets claimers take different rows instead of queuing for the same one.
3. With one process and one slot, SQLite beats PostgreSQL: an in-process library call costs less than a network round trip.
4. S4 throughput stays within 20% between an empty table and one with a million completed jobs.
5. *(Added after the review, which had already measured part of the effect, so this one isn't blind.)* Above roughly 1,000 pending jobs, claim throughput falls roughly in proportion to the backlog on both backends. Both databases handle the claim query's `OR` by collecting every ready row and sorting it (`MULTI-INDEX OR` + `TEMP B-TREE` on SQLite, `BitmapOr` + `Sort` on PostgreSQL). On SQLite this happens while holding the write lock, so at large backlogs other workers may wait past `busy_timeout` and the run fails its correctness check.

## Deliverables

- `bench/`: the runner (`npm run bench`), with flags for backend, scenario, repeats and smoke size. It is TypeScript run with `tsx` and needs no new dependencies.
- `bench/results/`: one JSON file per published run, with every run's raw numbers, committed, so the README's numbers can be traced to raw data.
- A "Performance" section in the README with a summary table, the machine, and a link to this doc.
- **CI smoke test:** every scenario at a tiny size on both backends, about a minute in total. It checks that every code path runs (including S3 taking its rates from S1), that every run passes the correctness check, and that every number is finite and positive. It publishes no numbers, because shared CI runners are too noisy to compare, and it can't show that a full-size run works.
- `test/bench-math.test.ts`: the arithmetic that turns timestamps into the published numbers, checked on inputs with known answers.

## Non-goals

- **Comparisons with other queues** (BullMQ, pg-boss, etc.). A fair comparison needs matched durability settings and features; maybe later, as its own piece of work.
- **Crashes under load.** Crash recovery has its own tests. Killing workers mid-run would make the "attempts = 1" check meaningless, so it would need a different correctness check.
- **Tuning.** These are baseline numbers. If one result points at a bottleneck, fixing it is a separate PR with before and after numbers. The claim query's handling of large backlogs (S5) is the obvious candidate, and this curve is its "before".

## Known limitations

- Autovacuum can start in the middle of a PostgreSQL run, since each run updates every row it creates. Repeats and the min–max range are the defense.
- S5's curve assumes FIFO claiming. If a curve isn't smooth, that assumption should be checked before the curve is published.

## Revisions after the review of the runner

An independent review of PR #42 (in a separate session, before any numbers were published) changed the method:

- **S1** is now measured at a fixed low backlog, since a whole-drain rate depends mostly on the preload size. **S5** is new, to report that dependence as a finding. **S3** takes its rates from the low-backlog number; before, its "75% load" was really about 20–25%.
- **Failures** in the S5 sweep are recorded rather than ending the run, with the rules above.
- **Correctness check:** added the handler-ran-exactly-once comparison.
- **Durability:** the review found SQLite running at NORMAL, which led to #43.
- **Harness:** a cheaper progress check; cleanup on interrupt; PostgreSQL 8 × 16 reported as not run.
- **Corrections to this doc:** timestamps come from the client clock, not PostgreSQL's `now()`, and claim wait means slightly different things on the two backends.

## Delivery plan (one PR each)

1. `docs: load test design` (this document, PR #41)
2. `feat(bench): load test runner with per-run correctness check` (PR #42: runner, S1–S5, CI smoke job)
3. #43's durability fix (PR #44), merged before any full-size run
4. `docs: load test results and README performance section`
