import { describe, expect, it } from "vitest";
import { JobStateError, type Storage } from "../src/index.js";

/**
 * Behavior every Storage backend must satisfy. Each backend's test file calls
 * this with a factory that returns a fresh, empty store.
 */
export function storageContract(name: string, create: () => Promise<Storage>): void {
  describe(`${name} storage contract`, () => {
    async function withStore(fn: (s: Storage) => Promise<void>): Promise<void> {
      const store = await create();
      try {
        await fn(store);
      } finally {
        await store.close();
      }
    }

    it("enqueues a job as pending with its payload intact", () =>
      withStore(async (s) => {
        const job = await s.enqueue({ queue: "email", payload: { to: "a@example.com", n: [1, 2] } });
        expect(job).toMatchObject({
          queue: "email",
          payload: { to: "a@example.com", n: [1, 2] },
          status: "pending",
          attempts: 0,
          workerId: null,
        });
        expect(await s.get(job.id)).toEqual(job);
      }));

    it("rejects an empty queue name and non-JSON payloads", () =>
      withStore(async (s) => {
        await expect(s.enqueue({ queue: "  ", payload: 1 })).rejects.toThrow(RangeError);
        await expect(s.enqueue({ queue: "q", payload: undefined })).rejects.toThrow(TypeError);
      }));

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

    it("completes a job held by the calling worker", () =>
      withStore(async (s) => {
        const { id } = await s.enqueue({ queue: "q", payload: 1 });
        await s.claim("q", "w1");
        await s.complete(id, "w1");
        expect(await s.get(id)).toMatchObject({ status: "completed" });
        expect((await s.get(id))?.finishedAt).not.toBeNull();
      }));

    it("fails a job held by the calling worker and records the error", () =>
      withStore(async (s) => {
        const { id } = await s.enqueue({ queue: "q", payload: 1 });
        await s.claim("q", "w1");
        await s.fail(id, "w1", "boom");
        expect(await s.get(id)).toMatchObject({ status: "failed", error: "boom" });
      }));

    it("refuses to complete or fail a job held by a different worker", () =>
      withStore(async (s) => {
        const { id } = await s.enqueue({ queue: "q", payload: 1 });
        await s.claim("q", "w1");
        await expect(s.complete(id, "w2")).rejects.toThrow(JobStateError);
        await expect(s.fail(id, "w2", "x")).rejects.toThrow(JobStateError);
        expect(await s.get(id)).toMatchObject({ status: "running", workerId: "w1" });
      }));

    it("refuses to complete a job that is not running", () =>
      withStore(async (s) => {
        const pending = await s.enqueue({ queue: "q", payload: 1 });
        await expect(s.complete(pending.id, "w1")).rejects.toThrow(JobStateError);

        await s.claim("q", "w1");
        await s.complete(pending.id, "w1");
        await expect(s.complete(pending.id, "w1")).rejects.toThrow(JobStateError);
      }));
  });
}
