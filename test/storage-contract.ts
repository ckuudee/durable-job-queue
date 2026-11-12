import { describe, expect, it } from "vitest";
import { IdempotencyConflictError, JobStateError, type RetryPolicy, type Storage } from "../src/index.js";

/** What a backend factory must honor so the contract can control time and backoff. */
export interface StorageTestOptions {
  now: () => number;
  retry: Partial<RetryPolicy>;
  /** Default lease length for jobs that don't set their own. */
  leaseMs: number;
}

const START = 1_000_000;
const LEASE = 10_000;
// No jitter: delays are exactly 1s, 2s, 4s, ... so tests can assert precise times.
const RETRY: Partial<RetryPolicy> = { baseDelayMs: 1_000, factor: 2, maxDelayMs: 60_000, jitter: false };

/**
 * Behavior every Storage backend must satisfy. Each backend's test file calls
 * this with a factory that returns a fresh, empty store.
 */
export function storageContract(
  name: string,
  create: (options: StorageTestOptions) => Promise<Storage>,
): void {
  describe(`${name} storage contract`, () => {
    async function withStore(fn: (s: Storage, clock: { t: number }) => Promise<void>): Promise<void> {
      const clock = { t: START };
      const store = await create({ now: () => clock.t, retry: RETRY, leaseMs: LEASE });
      try {
        await fn(store, clock);
      } finally {
        await store.close();
      }
    }

    describe("enqueue", () => {
      it("stores a pending job with its payload intact and default maxAttempts", () =>
        withStore(async (s) => {
          const job = await s.enqueue({ queue: "email", payload: { to: "a@example.com", n: [1, 2] } });
          expect(job).toMatchObject({
            queue: "email",
            payload: { to: "a@example.com", n: [1, 2] },
            status: "pending",
            attempts: 0,
            maxAttempts: 3,
            workerId: null,
            runAt: START,
            leaseMs: LEASE,
            leaseUntil: null,
            idempotencyKey: null,
          });
          expect(await s.get(job.id)).toEqual(job);
        }));

      it("rejects an empty queue name, bad maxAttempts or leaseMs, and non-JSON payloads", () =>
        withStore(async (s) => {
          await expect(s.enqueue({ queue: "  ", payload: 1 })).rejects.toThrow(RangeError);
          await expect(s.enqueue({ queue: "q", payload: undefined })).rejects.toThrow(TypeError);
          await expect(s.enqueue({ queue: "q", payload: 1, maxAttempts: 0 })).rejects.toThrow(RangeError);
          await expect(s.enqueue({ queue: "q", payload: 1, maxAttempts: 1.5 })).rejects.toThrow(RangeError);
          await expect(s.enqueue({ queue: "q", payload: 1, leaseMs: 0 })).rejects.toThrow(RangeError);
          await expect(s.enqueue({ queue: "q", payload: 1, leaseMs: 1.5 })).rejects.toThrow(RangeError);
        }));
    });

    describe("idempotency keys", () => {
      it("returns the existing job instead of creating a duplicate", () =>
        withStore(async (s) => {
          const first = await s.enqueue({ queue: "q", payload: { order: 7 }, idempotencyKey: "order-7" });
          const again = await s.enqueue({ queue: "q", payload: { order: 7 }, idempotencyKey: "order-7" });

          expect(again).toEqual(first);
          expect(first.idempotencyKey).toBe("order-7");
          expect(await s.claim("q", "w1")).toMatchObject({ id: first.id });
          expect(await s.claim("q", "w1")).toBeNull(); // only one job was ever created
        }));

      it("never deduplicates jobs that have no key", () =>
        withStore(async (s) => {
          const a = await s.enqueue({ queue: "q", payload: "same" });
          const b = await s.enqueue({ queue: "q", payload: "same" });
          expect(a.id).not.toBe(b.id);
        }));

      it("keeps different keys separate", () =>
        withStore(async (s) => {
          const a = await s.enqueue({ queue: "q", payload: 1, idempotencyKey: "a" });
          const b = await s.enqueue({ queue: "q", payload: 1, idempotencyKey: "b" });
          expect(a.id).not.toBe(b.id);
        }));

      it("scopes keys to a queue", () =>
        withStore(async (s) => {
          const a = await s.enqueue({ queue: "emails", payload: 1, idempotencyKey: "k" });
          const b = await s.enqueue({ queue: "billing", payload: 1, idempotencyKey: "k" });
          expect(a.id).not.toBe(b.id);
        }));

      it("returns the job's current state, so a finished job is not run again", () =>
        withStore(async (s) => {
          const { id } = await s.enqueue({ queue: "q", payload: 1, idempotencyKey: "k" });
          await s.claim("q", "w1");
          await s.complete(id, "w1");

          expect(await s.enqueue({ queue: "q", payload: 1, idempotencyKey: "k" })).toMatchObject({
            id,
            status: "completed",
          });
          expect(await s.claim("q", "w1")).toBeNull();
        }));

      it("does not resurrect a dead job; requeueDead is the explicit way back", () =>
        withStore(async (s) => {
          const { id } = await s.enqueue({ queue: "q", payload: 1, maxAttempts: 1, idempotencyKey: "k" });
          await s.claim("q", "w1");
          await s.fail(id, "w1", "boom");

          expect(await s.enqueue({ queue: "q", payload: 1, idempotencyKey: "k" })).toMatchObject({
            id,
            status: "dead",
          });
          expect(await s.claim("q", "w1")).toBeNull();
        }));

      it("keeps the first call's maxAttempts and leaseMs", () =>
        withStore(async (s) => {
          await s.enqueue({ queue: "q", payload: 1, maxAttempts: 5, leaseMs: 2_000, idempotencyKey: "k" });
          const again = await s.enqueue({ queue: "q", payload: 1, maxAttempts: 1, leaseMs: 9_000, idempotencyKey: "k" });
          expect(again).toMatchObject({ maxAttempts: 5, leaseMs: 2_000 });
        }));

      it("rejects a reused key with a different payload", () =>
        withStore(async (s) => {
          await s.enqueue({ queue: "q", payload: { amount: 100 }, idempotencyKey: "charge-1" });
          await expect(
            s.enqueue({ queue: "q", payload: { amount: 999 }, idempotencyKey: "charge-1" }),
          ).rejects.toThrow(IdempotencyConflictError);
        }));

      it("treats payloads that differ only in object key order as the same", () =>
        withStore(async (s) => {
          const first = await s.enqueue({ queue: "q", payload: { a: 1, b: { c: 2, d: [3, { e: 4, f: 5 }] } }, idempotencyKey: "k" });
          const again = await s.enqueue({ queue: "q", payload: { b: { d: [3, { f: 5, e: 4 }], c: 2 }, a: 1 }, idempotencyKey: "k" });
          expect(again.id).toBe(first.id);
        }));

      it("rejects blank or oversized keys", () =>
        withStore(async (s) => {
          await expect(s.enqueue({ queue: "q", payload: 1, idempotencyKey: "" })).rejects.toThrow(RangeError);
          await expect(s.enqueue({ queue: "q", payload: 1, idempotencyKey: "   " })).rejects.toThrow(RangeError);
          await expect(
            s.enqueue({ queue: "q", payload: 1, idempotencyKey: "x".repeat(256) }),
          ).rejects.toThrow(RangeError);
        }));

      it("creates exactly one job when many callers race with the same key", () =>
        withStore(async (s) => {
          const jobs = await Promise.all(
            Array.from({ length: 10 }, () => s.enqueue({ queue: "q", payload: 1, idempotencyKey: "k" })),
          );
          expect(new Set(jobs.map((j) => j.id)).size).toBe(1);
        }));
    });

    describe("delayed jobs", () => {
      it("holds a job back until delayMs has passed", () =>
        withStore(async (s, clock) => {
          const job = await s.enqueue({ queue: "q", payload: 1, delayMs: 5_000 });
          expect(job).toMatchObject({ status: "pending", runAt: START + 5_000, createdAt: START });

          clock.t = START + 4_999;
          expect(await s.claim("q", "w1")).toBeNull();
          clock.t = START + 5_000;
          expect(await s.claim("q", "w1")).toMatchObject({ id: job.id, status: "running" });
        }));

      it("holds a job back until an absolute runAt, given as epoch ms or a Date", () =>
        withStore(async (s, clock) => {
          const a = await s.enqueue({ queue: "q", payload: "a", runAt: START + 2_000 });
          const b = await s.enqueue({ queue: "q", payload: "b", runAt: new Date(START + 3_000) });
          expect(b.runAt).toBe(START + 3_000);

          clock.t = START + 2_000;
          expect((await s.claim("q", "w1"))?.id).toBe(a.id);
          expect(await s.claim("q", "w1")).toBeNull();
          clock.t = START + 3_000;
          expect((await s.claim("q", "w1"))?.id).toBe(b.id);
        }));

      it("treats a runAt in the past, or a zero delay, as ready now", () =>
        withStore(async (s) => {
          const past = await s.enqueue({ queue: "q", payload: 1, runAt: START - 60_000 });
          const zero = await s.enqueue({ queue: "q", payload: 2, delayMs: 0 });
          expect((await s.claim("q", "w1"))?.id).toBe(past.id);
          expect((await s.claim("q", "w1"))?.id).toBe(zero.id);
        }));

      it("doesn't let a delayed job hold up ready jobs behind it", () =>
        withStore(async (s) => {
          await s.enqueue({ queue: "q", payload: "later", delayMs: 60_000 });
          const now = await s.enqueue({ queue: "q", payload: "now" });
          expect((await s.claim("q", "w1"))?.id).toBe(now.id);
          expect(await s.claim("q", "w1")).toBeNull();
        }));

      it("runs due jobs in order of their scheduled time, not enqueue order", () =>
        withStore(async (s, clock) => {
          const late = await s.enqueue({ queue: "q", payload: "late", delayMs: 2_000 });
          const early = await s.enqueue({ queue: "q", payload: "early", delayMs: 1_000 });
          clock.t = START + 10_000;
          expect((await s.claim("q", "w1"))?.id).toBe(early.id);
          expect((await s.claim("q", "w1"))?.id).toBe(late.id);
        }));

      it("rejects both options at once, or an invalid time or delay", () =>
        withStore(async (s) => {
          const bad = [
            { runAt: START, delayMs: 0 },
            { delayMs: -1 },
            { delayMs: 1.5 },
            { delayMs: Number.POSITIVE_INFINITY },
            { runAt: Number.NaN },
            { runAt: new Date("not a date") },
          ];
          for (const options of bad) {
            await expect(s.enqueue({ queue: "q", payload: 1, ...options })).rejects.toThrow(RangeError);
          }
          expect(await s.claim("q", "w1")).toBeNull(); // nothing was stored
        }));

      it("keeps the first call's schedule when an idempotency key is reused", () =>
        withStore(async (s) => {
          await s.enqueue({ queue: "q", payload: 1, delayMs: 5_000, idempotencyKey: "k" });
          const again = await s.enqueue({ queue: "q", payload: 1, delayMs: 0, idempotencyKey: "k" });
          expect(again.runAt).toBe(START + 5_000);
          expect(await s.claim("q", "w1")).toBeNull();
        }));
    });

    describe("claim", () => {
      it("returns null when claiming from an empty queue", () =>
        withStore(async (s) => {
          expect(await s.claim("q", "w1")).toBeNull();
        }));

      it("claims jobs in FIFO order", () =>
        withStore(async (s) => {
          const a = await s.enqueue({ queue: "q", payload: "a" });
          const b = await s.enqueue({ queue: "q", payload: "b" });
          expect((await s.claim("q", "w1"))?.id).toBe(a.id);
          expect((await s.claim("q", "w1"))?.id).toBe(b.id);
          expect(await s.claim("q", "w1")).toBeNull();
        }));

      it("moves a claimed job to running and records the worker", () =>
        withStore(async (s) => {
          await s.enqueue({ queue: "q", payload: 1 });
          const claimed = await s.claim("q", "w1");
          expect(claimed).toMatchObject({ status: "running", workerId: "w1", attempts: 1 });
          expect(claimed?.claimedAt).not.toBeNull();
        }));

      it("never hands the same job to two claimers", () =>
        withStore(async (s) => {
          await s.enqueue({ queue: "q", payload: 1 });
          const results = await Promise.all([s.claim("q", "w1"), s.claim("q", "w2")]);
          expect(results.filter((r) => r !== null)).toHaveLength(1);
        }));

      it("only claims from the requested queue", () =>
        withStore(async (s) => {
          await s.enqueue({ queue: "other", payload: 1 });
          expect(await s.claim("q", "w1")).toBeNull();
          expect(await s.claim("other", "w1")).not.toBeNull();
        }));
    });

    describe("complete", () => {
      it("completes a job held by the calling worker", () =>
        withStore(async (s) => {
          const { id } = await s.enqueue({ queue: "q", payload: 1 });
          await s.claim("q", "w1");
          await s.complete(id, "w1");
          expect(await s.get(id)).toMatchObject({ status: "completed" });
          expect((await s.get(id))?.finishedAt).not.toBeNull();
        }));

      it("refuses a different worker, or a job that is not running", () =>
        withStore(async (s) => {
          const { id } = await s.enqueue({ queue: "q", payload: 1 });
          await expect(s.complete(id, "w1")).rejects.toThrow(JobStateError); // still pending

          await s.claim("q", "w1");
          await expect(s.complete(id, "w2")).rejects.toThrow(JobStateError); // wrong worker
          expect(await s.get(id)).toMatchObject({ status: "running", workerId: "w1" });

          await s.complete(id, "w1");
          await expect(s.complete(id, "w1")).rejects.toThrow(JobStateError); // already done
        }));
    });

    describe("fail and retry", () => {
      it("sends a failed job back to pending with exponential backoff", () =>
        withStore(async (s, clock) => {
          const { id } = await s.enqueue({ queue: "q", payload: 1, maxAttempts: 4 });

          for (const [attempt, delay] of [[1, 1_000], [2, 2_000], [3, 4_000]] as const) {
            const claimed = await s.claim("q", "w1");
            expect(claimed?.attempts).toBe(attempt);

            const failed = await s.fail(id, "w1", `boom ${attempt}`);
            expect(failed).toMatchObject({
              status: "pending",
              attempts: attempt,
              error: `boom ${attempt}`,
              workerId: null,
              runAt: clock.t + delay,
            });
            clock.t += delay;
          }
        }));

      it("does not make a retried job claimable until its backoff elapses", () =>
        withStore(async (s, clock) => {
          const { id } = await s.enqueue({ queue: "q", payload: 1 });
          await s.claim("q", "w1");
          await s.fail(id, "w1", "x"); // retry in 1000 ms

          clock.t += 999;
          expect(await s.claim("q", "w2")).toBeNull();
          clock.t += 1;
          expect(await s.claim("q", "w2")).toMatchObject({ id, attempts: 2 });
        }));

      it("lets other ready jobs run while a failed one waits out its backoff", () =>
        withStore(async (s, clock) => {
          const first = await s.enqueue({ queue: "q", payload: "first" });
          await s.claim("q", "w1");
          await s.fail(first.id, "w1", "x");

          clock.t += 10;
          const second = await s.enqueue({ queue: "q", payload: "second" });
          expect((await s.claim("q", "w1"))?.id).toBe(second.id);
        }));

      it("moves a job to dead once attempt maxAttempts fails", () =>
        withStore(async (s, clock) => {
          const { id } = await s.enqueue({ queue: "q", payload: 1, maxAttempts: 2 });

          await s.claim("q", "w1");
          expect(await s.fail(id, "w1", "first")).toMatchObject({ status: "pending" });
          clock.t += 1_000;

          await s.claim("q", "w1");
          const dead = await s.fail(id, "w1", "last");
          expect(dead).toMatchObject({ status: "dead", attempts: 2, error: "last", finishedAt: clock.t });

          clock.t += 1_000_000;
          expect(await s.claim("q", "w1")).toBeNull();
        }));

      it("makes a job dead on its first failure when maxAttempts is 1", () =>
        withStore(async (s) => {
          const { id } = await s.enqueue({ queue: "q", payload: 1, maxAttempts: 1 });
          await s.claim("q", "w1");
          expect(await s.fail(id, "w1", "no retries")).toMatchObject({ status: "dead", attempts: 1 });
        }));

      it("refuses to fail a job held by a different worker or not running", () =>
        withStore(async (s) => {
          const { id } = await s.enqueue({ queue: "q", payload: 1 });
          await expect(s.fail(id, "w1", "x")).rejects.toThrow(JobStateError); // pending

          await s.claim("q", "w1");
          await expect(s.fail(id, "w2", "x")).rejects.toThrow(JobStateError);
          expect(await s.get(id)).toMatchObject({ status: "running", workerId: "w1", attempts: 1 });
        }));
    });

    describe("leases and crash recovery", () => {
      it("gives a claimed job a lease, using the job's own leaseMs when set", () =>
        withStore(async (s) => {
          await s.enqueue({ queue: "default-lease", payload: 1 });
          await s.enqueue({ queue: "short-lease", payload: 1, leaseMs: 2_000 });
          expect((await s.claim("default-lease", "w1"))?.leaseUntil).toBe(START + LEASE);
          expect((await s.claim("short-lease", "w1"))?.leaseUntil).toBe(START + 2_000);
        }));

      it("clears the lease when a job completes, retries or dies", () =>
        withStore(async (s) => {
          const done = await s.enqueue({ queue: "q", payload: "done" });
          await s.claim("q", "w1");
          await s.complete(done.id, "w1");
          expect((await s.get(done.id))?.leaseUntil).toBeNull();

          const retry = await s.enqueue({ queue: "q", payload: "retry" });
          await s.claim("q", "w1");
          expect((await s.fail(retry.id, "w1", "x")).leaseUntil).toBeNull();

          const dead = await s.enqueue({ queue: "q", payload: "dead", maxAttempts: 1 });
          await s.claim("q", "w1");
          expect((await s.fail(dead.id, "w1", "x")).leaseUntil).toBeNull();
        }));

      it("extends the lease on heartbeat", () =>
        withStore(async (s, clock) => {
          const { id } = await s.enqueue({ queue: "q", payload: 1 });
          await s.claim("q", "w1");
          clock.t += 6_000;
          expect(await s.heartbeat(id, "w1")).toMatchObject({ status: "running", leaseUntil: clock.t + LEASE });
        }));

      it("refuses a heartbeat from a different worker or for a job that is not running", () =>
        withStore(async (s) => {
          const { id } = await s.enqueue({ queue: "q", payload: 1 });
          await expect(s.heartbeat(id, "w1")).rejects.toThrow(JobStateError); // pending

          await s.claim("q", "w1");
          await expect(s.heartbeat(id, "w2")).rejects.toThrow(JobStateError); // wrong worker
        }));

      it("keeps a job from other workers until its lease lapses", () =>
        withStore(async (s, clock) => {
          await s.enqueue({ queue: "q", payload: 1 });
          await s.claim("q", "w1");

          clock.t = START + LEASE - 1;
          expect(await s.claim("q", "w2")).toBeNull();
        }));

      it("hands a job to another worker once its lease lapses", () =>
        withStore(async (s, clock) => {
          const { id } = await s.enqueue({ queue: "q", payload: 1 });
          await s.claim("q", "w1");

          clock.t = START + LEASE;
          const reclaimed = await s.claim("q", "w2");
          expect(reclaimed).toMatchObject({
            id,
            status: "running",
            workerId: "w2",
            attempts: 2,
            leaseUntil: clock.t + LEASE,
          });
          expect(reclaimed?.error).toMatch(/lease expired \(worker w1\)/);
        }));

      it("keeps a job away from others for as long as its worker heartbeats", () =>
        withStore(async (s, clock) => {
          const { id } = await s.enqueue({ queue: "q", payload: 1 });
          await s.claim("q", "w1");

          clock.t = START + 9_000;
          await s.heartbeat(id, "w1"); // lease now runs to START + 19_000
          clock.t = START + LEASE;
          expect(await s.claim("q", "w2")).toBeNull();
          clock.t = START + 18_999;
          expect(await s.claim("q", "w2")).toBeNull();

          clock.t = START + 19_000;
          expect(await s.claim("q", "w2")).toMatchObject({ id, workerId: "w2" });
        }));

      it("rejects a zombie worker whose job was reclaimed", () =>
        withStore(async (s, clock) => {
          const { id } = await s.enqueue({ queue: "q", payload: 1 });
          await s.claim("q", "zombie");

          clock.t = START + LEASE;
          await s.claim("q", "w2"); // takes over

          await expect(s.complete(id, "zombie")).rejects.toThrow(JobStateError);
          await expect(s.fail(id, "zombie", "late")).rejects.toThrow(JobStateError);
          await expect(s.heartbeat(id, "zombie")).rejects.toThrow(JobStateError);
          expect(await s.get(id)).toMatchObject({ status: "running", workerId: "w2" });

          await s.complete(id, "w2");
          expect(await s.get(id)).toMatchObject({ status: "completed", workerId: "w2" });
        }));

      it("counts an abandoned attempt, and kills a job that keeps getting abandoned", () =>
        withStore(async (s, clock) => {
          const { id } = await s.enqueue({ queue: "q", payload: 1, maxAttempts: 2 });

          await s.claim("q", "w1"); // attempt 1, abandoned
          clock.t += LEASE;
          expect(await s.claim("q", "w2")).toMatchObject({ attempts: 2 }); // attempt 2, abandoned
          clock.t += LEASE;

          expect(await s.claim("q", "w3")).toBeNull(); // out of attempts
          expect(await s.get(id)).toMatchObject({
            status: "dead",
            attempts: 2,
            finishedAt: clock.t,
            leaseUntil: null,
          });
          expect((await s.get(id))?.error).toMatch(/lease expired \(worker w2\) after 2 attempts/);
          expect(await s.listDead("q")).toHaveLength(1);
        }));

      it("still accepts a lapsed worker's result if nobody has taken the job yet", () =>
        withStore(async (s, clock) => {
          const { id } = await s.enqueue({ queue: "q", payload: 1 });
          await s.claim("q", "w1");

          clock.t += 60_000; // lease long gone, but no other worker has claimed it
          await s.complete(id, "w1");
          expect(await s.get(id)).toMatchObject({ status: "completed" });
          expect(await s.claim("q", "w2")).toBeNull();
        }));

      it("lets a lapsed worker heartbeat its way back if nobody has taken the job yet", () =>
        withStore(async (s, clock) => {
          const { id } = await s.enqueue({ queue: "q", payload: 1 });
          await s.claim("q", "w1");

          clock.t += 15_000;
          await s.heartbeat(id, "w1");
          expect(await s.claim("q", "w2")).toBeNull();
        }));

      it("only recovers abandoned jobs in the queue being claimed", () =>
        withStore(async (s, clock) => {
          await s.enqueue({ queue: "q", payload: 1 });
          await s.claim("q", "w1");

          clock.t += LEASE;
          expect(await s.claim("other", "w2")).toBeNull();
        }));
    });

    describe("dead-letter queue", () => {
      async function killJob(s: Storage, queue: string, payload: unknown): Promise<string> {
        const { id } = await s.enqueue({ queue, payload, maxAttempts: 1 });
        await s.claim(queue, "w1");
        await s.fail(id, "w1", "dead");
        return id;
      }

      it("lists only dead jobs for the requested queue, oldest failure first", () =>
        withStore(async (s, clock) => {
          const a = await killJob(s, "q", "a");
          clock.t += 5;
          const b = await killJob(s, "q", "b");
          await killJob(s, "other", "c");
          await s.enqueue({ queue: "q", payload: "still pending" });

          expect((await s.listDead("q")).map((j) => j.id)).toEqual([a, b]);
          expect(await s.listDead("q", 1)).toHaveLength(1);
        }));

      it("requeues a dead job with a fresh attempt count", () =>
        withStore(async (s, clock) => {
          const id = await killJob(s, "q", 1);
          clock.t += 500;

          const requeued = await s.requeueDead(id);
          expect(requeued).toMatchObject({
            status: "pending",
            attempts: 0,
            workerId: null,
            finishedAt: null,
            runAt: clock.t,
          });
          expect(await s.listDead("q")).toHaveLength(0);
          expect(await s.claim("q", "w2")).toMatchObject({ id, attempts: 1 });
        }));

      it("refuses to requeue a job that is not dead", () =>
        withStore(async (s) => {
          const { id } = await s.enqueue({ queue: "q", payload: 1 });
          await expect(s.requeueDead(id)).rejects.toThrow(JobStateError);
          await expect(s.requeueDead("no-such-id")).rejects.toThrow(JobStateError);
        }));
    });
  });
}
