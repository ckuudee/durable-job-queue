import { describe, expect, it } from "vitest";
import { SqliteStorage } from "../src/index.js";
import { storageContract } from "./storage-contract.js";

storageContract(
  "SQLite (in-memory)",
  async ({ now, retry }) => new SqliteStorage({ path: ":memory:", now, retry }),
);

describe("SqliteStorage clock", () => {
  it("stamps jobs using the injected clock", async () => {
    let t = 1_000;
    const store = new SqliteStorage({ path: ":memory:", now: () => t });

    const job = await store.enqueue({ queue: "q", payload: 1 });
    expect(job.createdAt).toBe(1_000);

    t = 2_500;
    const claimed = await store.claim("q", "w1");
    expect(claimed?.claimedAt).toBe(2_500);

    t = 4_000;
    await store.complete(job.id, "w1");
    expect((await store.get(job.id))?.finishedAt).toBe(4_000);
    await store.close();
  });
});

describe("SqliteStorage retry options", () => {
  it("applies jitter from the injected random source", async () => {
    const store = new SqliteStorage({
      path: ":memory:",
      now: () => 0,
      retry: { baseDelayMs: 1_000, jitter: true },
      random: () => 0,
    });
    const { id } = await store.enqueue({ queue: "q", payload: 1 });
    await store.claim("q", "w1");
    expect((await store.fail(id, "w1", "x")).runAt).toBe(500); // lower bound of [500, 1000]
    await store.close();
  });

  it("rejects an invalid retry policy at construction", () => {
    expect(() => new SqliteStorage({ path: ":memory:", retry: { factor: 0.5 } })).toThrow(RangeError);
  });
});
