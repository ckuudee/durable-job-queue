import { describe, expect, it } from "vitest";
import { SqliteStorage } from "../src/index.js";
import { storageContract } from "./storage-contract.js";

storageContract("SQLite (in-memory)", async () => new SqliteStorage({ path: ":memory:" }));

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
