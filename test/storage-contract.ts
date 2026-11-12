import { describe, expect, it } from "vitest";
import { JobStateError, type RetryPolicy, type Storage } from "../src/index.js";

/** What a backend factory must honor so the contract can control time and backoff. */
export interface StorageTestOptions {
  now: () => number;
  retry: Partial<RetryPolicy>;
}

const START = 1_000_000;
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
      const store = await create({ now: () => clock.t, retry: RETRY });
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
          });
          expect(await s.get(job.id)).toEqual(job);
        }));

      it("rejects an empty queue name, bad maxAttempts and non-JSON payloads", () =>
        withStore(async (s) => {
          await expect(s.enqueue({ queue: "  ", payload: 1 })).rejects.toThrow(RangeError);
          await expect(s.enqueue({ queue: "q", payload: undefined })).rejects.toThrow(TypeError);
          await expect(s.enqueue({ queue: "q", payload: 1, maxAttempts: 0 })).rejects.toThrow(RangeError);
          await expect(s.enqueue({ queue: "q", payload: 1, maxAttempts: 1.5 })).rejects.toThrow(RangeError);
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
