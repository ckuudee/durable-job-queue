# Guarantees under adversarial test

Written from the spec only (README.md, docs/design/metrics.md, and the doc
comments on the public API) **before** reading implementation bodies or the
existing tests. Each ID is referenced from the test that attacks it.

Sources: `R` = README.md, `M` = docs/design/metrics.md, `S` = src/storage.ts,
`T` = src/types.ts, `W` = src/worker.ts, `SC` = src/scheduler.ts,
`D` = src/dashboard.ts, `B` = src/backoff.ts.

## Storage

| ID | Guarantee | Source |
|----|-----------|--------|
| S1 | A ready job is handed to exactly one worker, even when many workers (separate processes) call `claim` at once. | S:27, R:13, R:133 |
| S2 | `claim` takes the *oldest* ready job. | S:49 |
| S3 | Ready = pending and `runAt` has passed. Boundary: `run_at <= now` is ready. | S:51, M:71 |
| S4 | A running job whose lease lapsed is claimable; boundary `lease_until <= now` ("same boundary as claim()"). | S:51, M:74 |
| S5 | A lapsed job that used all attempts is marked dead by `claim` instead of being returned. | S:52, T:17 |
| S6 | `attempts` = number of times the job was claimed. | T:15 |
| S7 | `heartbeat` extends the lease by the job's `leaseMs`; throws JobStateError if the job was lost. | S:57-61 |
| S8 | `complete`/`fail` throw JobStateError unless `workerId` holds the job; a lapsed-but-not-reclaimed holder can still complete. | S:64-77, R:133 |
| S9 | `fail` with attempts left → pending, `runAt` pushed back by backoff; else dead. Returns the updated job. | S:71-77 |
| S10 | `listDead` returns dead jobs oldest failure first; honours `limit`. | S:79 |
| S11 | `requeueDead` gives a fresh attempt count; throws JobStateError unless dead. | S:82 |
| S12 | Idempotent enqueue: same (queue, key) returns the existing job in any state, creates nothing; first call's `maxAttempts`, `leaseMs`, schedule win. | S:37-45, T:85-90 |
| S13 | Same key with a *different* payload throws IdempotencyConflictError; "same" is canonical JSON (key order irrelevant). | T:88, idempotency.ts canonicalize doc |
| S14 | Key is unique *within the queue*: same key in another queue is a separate job. | T:29 |
| S15 | Exactly one of any number of concurrent enqueuers with the same key creates the job. | S:41 |
| S16 | Payload can be any JSON-serializable value and round-trips. | R:80, T:12 |
| S17 | `runAt` in the past means run now; `runAt` (number or Date) and `delayMs` are mutually exclusive. | T:79-84 |
| S18 | Defaults: 3 attempts, 30 s lease (store-level `leaseMs` overrides). | R:76, T:74-77 |
| S19 | SQLite and Postgres behave identically. | R:62 |

## Backoff

| ID | Guarantee | Source |
|----|-----------|--------|
| B1 | Delay after the n-th failure = base·factorⁿ⁻¹, capped at `maxDelayMs`. | B:28-31 |
| B2 | With jitter the delay is in [delay/2, delay]: never immediate. | B:29-31 |

## stats()

| ID | Guarantee | Source |
|----|-----------|--------|
| ST1 | One consistent snapshot; counts never double-count or miss a job changing state. | S:88, T:43, M:66 |
| ST2 | Sorted by queue name; queues with no jobs are not listed. | S:89-90 |
| ST3 | Same clock and lease boundary as `claim`: a job counted `stalled` is exactly one `claim` would take over. | S:90-91 |
| ST4 | `oldestReadyAgeMs` = now − runAt of the oldest *ready* job, null if none. | T:61-66 |
| ST5 | `at` is the store clock reading. | T:67 |

## Worker

| ID | Guarantee | Source |
|----|-----------|--------|
| W1 | Every `job:claimed` is followed by exactly one of completed / failed / abandoned. | W:8-10, M:113 |
| W2 | `job` in each event is the job as it was claimed. | W:10 |
| W3 | `job:completed` emitted after completion is recorded; `job:failed` after failure recorded, `willRetry` false iff dead. | W:21-23 |
| W4 | A throwing (sync or async) listener goes to `onError` and doesn't break the job. | W:269-275, M:123 |
| W5 | At most `concurrency` handlers run at once. | W:70 |
| W6 | `start()` again while running does nothing; `stop()` safe when not running; stop waits for in-flight jobs. | W:148, W:156-159 |
| W7 | `stop({timeoutMs})` aborts signals and records no result; the job runs again elsewhere. | W:85-91 |
| W8 | Handler errors are not reported to `onError`; they fail the job. | W:79 |
| W9 | Heartbeats keep a handler that runs longer than `leaseMs` from losing its job. | R:100, W:99 |

## Scheduler

| ID | Guarantee | Source |
|----|-----------|--------|
| SC1 | `add` throws RangeError for a bad name (> 200 chars), cron expression or time zone. | SC:84, SC:5 |
| SC2 | `tick` enqueues every occurrence in (now, now + lookahead], and always at least the next one. | SC:103-106 |
| SC3 | Any number of schedulers / ticks → each occurrence becomes exactly one job. | SC:49-52, R:18 |
| SC4 | After downtime: already-enqueued occurrence runs late, fully-missed occurrences are skipped. | SC:53-55, R:128 |
| SC5 | Payload change takes effect from the first not-yet-enqueued occurrence (no IdempotencyConflictError). | SC:57-58 |
| SC6 | Time zones (including DST transitions) are honoured. | SC:19, R:18 |
| SC7 | `stop()` resolves once any in-flight tick has finished; ticks never overlap. | SC:120, SC:142 |

## Dashboard

| ID | Guarantee | Source |
|----|-----------|--------|
| D1 | `GET /api/stats` → `storage.stats()` as JSON; `GET /` → HTML 200. | M:141-142 |
| D2 | Unknown routes → 404; non-GET methods → 405. | M:153 |
| D3 | Host header must be one of localhost, 127.0.0.1, [::1], `host` (port stripped) — blocks DNS rebinding. | D:16-22 |
| D4 | Binds 127.0.0.1 by default. | D:10-14, M:147 |
| D5 | Dead-letter list omits payloads. | D:39 |
| D6 | `close()` drops open connections, including requests still waiting on storage. | D:33 |

## Findings

Each confirmed bug's test is wrapped in `knownBug(issue, …)` (helpers/known-bug.ts),
i.e. `it.fails` on the affected backends. When a fix lands, that test fails
("expected to fail"): remove the wrapper and close the issue.

| Issue | Severity | Guarantee broken | Backends |
|-------|----------|------------------|----------|
| #16 (fixed) | high | W7: stop() timeout keeps heartbeating a stuck job, so its lease never lapses | both |
| #17 (fixed) | high | S1/S8: lease fenced by workerId only; one Worker can run a job twice at once | both |
| #18 | medium | S16/S19: U+0000 / lone surrogates rejected in payloads and error text | PostgreSQL |
| #19 | medium | SC6: daily job runs twice on the DST fall-back day | both |
| #20 | medium-low | SC7: stop() + start() lets ticks overlap; a tick runs after stop() resolves | both |
| #21 | medium-low | W6: a second concurrent stop() doesn't wait for in-flight jobs | both |
| #22 | medium-low | B1/S9: fractional no-jitter backoff, so PostgreSQL fail() throws | both (throws on PG) |
| #23 | low | B1/S9: baseDelayMs 0 with more than 1024 attempts gives NaN runAt | both |

Held up under attack: S1–S5 multi-process claiming (including constant lease
lapses), runAt/leaseUntil boundaries, ST1 snapshot consistency under concurrent
change, idempotency canonicalization (numbers, -0, Unicode, key order), and the
dashboard's Host/method/path handling (D1–D4).

## Ambiguities noticed while reading the spec

- A1: Is "oldest" in `claim` by `runAt` or `createdAt`?
- A2: `stats("empty-queue")`: `[]` (S:89-90) vs a row with `oldestReadyAgeMs: null` (M:95)?
- A3: Should `Host: LOCALHOST` or `localhost.` (trailing dot) be accepted? DNS names are case-insensitive.
- A4: Duplicate schedule `name` in one Scheduler — error or allowed?
- A5: Does a lapsed-lease holder that was *not* re-claimed get to `fail` too (S:65 says complete only)?
