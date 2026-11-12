# durable-job-queue

A durable background job queue for Node.js: jobs survive crashes, failed jobs retry with backoff, and the system reports what it is doing.

> Status: early scaffold. Nothing is implemented yet.

## Why

Most web apps need to do slow or unreliable work (send email, call a third-party API, process a file) outside the request. Doing that safely is harder than it looks: workers crash mid-job, two workers can grab the same job, and retries can run an action twice. This project is a from-scratch implementation that treats those failure cases as the main requirement.

## Planned features

- **Durable storage**: jobs persist in a database and are claimed atomically so no two workers run the same job.
- **Retries with exponential backoff** and a **dead-letter queue** for jobs that keep failing.
- **Scheduling**: delayed and recurring jobs.
- **Idempotency keys** so a retried or duplicated enqueue does not run twice.
- **Crash recovery**: jobs held by a dead worker are detected (lease timeout) and re-queued.
- **Observability**: metrics and a small dashboard (queue depth, throughput, failure rate).
- **Measured performance**: load tests with published numbers, not claims.

## Design goal

Correctness first. Each guarantee above gets a test that tries to break it (crash a worker mid-job, enqueue concurrently, kill the process), and a short design doc explains the tradeoffs, including what this queue does *not* guarantee.

## Stack

TypeScript, Node.js, a SQL database (SQLite first behind a storage interface; PostgreSQL with `FOR UPDATE SKIP LOCKED` as the concurrent backend), Vitest.

## Roadmap

- [x] Tooling and CI
- [x] Storage interface and SQLite backend
- [x] Enqueue, atomic claim, complete/fail
- [x] Retries, backoff, dead-letter queue
- [ ] Leases and crash recovery
- [ ] Delayed and recurring jobs
- [ ] Idempotency keys
- [ ] PostgreSQL backend
- [ ] Metrics and dashboard
- [ ] Load tests and design doc

## License

MIT
