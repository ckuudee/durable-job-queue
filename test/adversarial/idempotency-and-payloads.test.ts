// Idempotency-key and payload edge cases. The spec says payloads can be "any
// JSON-serializable value" (README "Enqueue jobs", types.ts Job.payload) and
// both backends "pass the same test suite" (README "Pick a storage backend").
import { afterEach, describe, expect, it } from "vitest";
import { IdempotencyConflictError } from "../../src/index.js";
import { BACKENDS, FakeClock, type Opened } from "./helpers/backends.js";
import { knownBug } from "./helpers/known-bug.js";

describe.each(BACKENDS)("$name: idempotency keys", (backend) => {
  let opened: Opened | undefined;
  afterEach(async () => {
    await opened?.cleanup();
    opened = undefined;
  });
  const open = async (clock = new FakeClock()) => (opened = await backend.open({ now: clock.now })).storage;

  it("S13: -0 and 0 are the same JSON payload (JSON has no -0)", async () => {
    const s = await open();
    const first = await s.enqueue({ queue: "q", idempotencyKey: "z", payload: { n: -0 } });
    expect((await s.enqueue({ queue: "q", idempotencyKey: "z", payload: { n: 0 } })).id).toBe(first.id);
  });

  it("S13/S16: large and extreme numbers round-trip and compare equal to themselves", async () => {
    const s = await open();
    const payload = {
      maxSafe: Number.MAX_SAFE_INTEGER,
      beyondSafe: 2 ** 53 + 2,
      int64ish: 2 ** 63,
      huge: 1e21,
      max: Number.MAX_VALUE,
      tiny: Number.MIN_VALUE,
      neg: -1.5e-7,
      frac: 0.1 + 0.2,
    };
    const first = await s.enqueue({ queue: "q", idempotencyKey: "nums", payload });
    expect(first.payload).toEqual(payload);
    expect((await s.get(first.id))?.payload).toEqual(payload);
    expect((await s.enqueue({ queue: "q", idempotencyKey: "nums", payload: { ...payload } })).id).toBe(first.id);
    // One ulp apart must be a conflict, not silently "equal".
    await expect(s.enqueue({ queue: "q", idempotencyKey: "nums", payload: { ...payload, frac: 0.3 } }))
      .rejects.toBeInstanceOf(IdempotencyConflictError);
  });

  it("S13/S16: unicode keys and values (NFC vs NFD are different payloads)", async () => {
    const s = await open();
    const nfc = "café";
    const nfd = "café";
    const first = await s.enqueue({ queue: "q", idempotencyKey: "u", payload: { [nfc]: "\u{1F600}", z: " " } });
    expect(first.payload).toEqual({ [nfc]: "\u{1F600}", z: " " });
    expect((await s.enqueue({ queue: "q", idempotencyKey: "u", payload: { z: " ", [nfc]: "\u{1F600}" } })).id).toBe(first.id);
    await expect(s.enqueue({ queue: "q", idempotencyKey: "u", payload: { [nfd]: "\u{1F600}", z: " " } }))
      .rejects.toBeInstanceOf(IdempotencyConflictError);
  });

  it("S12: emoji key at the 255 UTF-16 unit limit is accepted and deduplicates", async () => {
    const s = await open();
    const key = "\u{1F600}".repeat(127) + "x"; // 255 UTF-16 code units
    expect(key.length).toBe(255);
    const a = await s.enqueue({ queue: "q", idempotencyKey: key, payload: 1 });
    expect((await s.enqueue({ queue: "q", idempotencyKey: key, payload: 1 })).id).toBe(a.id);
    await expect(s.enqueue({ queue: "q", idempotencyKey: key + "y", payload: 1 })).rejects.toBeInstanceOf(RangeError);
  });

  it("S14: the same key in two queues makes two jobs; keys differing only in case or whitespace are distinct", async () => {
    const s = await open();
    const a = await s.enqueue({ queue: "a", idempotencyKey: "k", payload: 1 });
    const b = await s.enqueue({ queue: "b", idempotencyKey: "k", payload: 2 });
    expect(a.id).not.toBe(b.id);
    const upper = await s.enqueue({ queue: "a", idempotencyKey: "K", payload: 3 });
    const spaced = await s.enqueue({ queue: "a", idempotencyKey: "k ", payload: 4 });
    expect(new Set([a.id, upper.id, spaced.id]).size).toBe(3);
  });

  it("S12: a repeat returns the job in its current state (completed, dead, requeued) and first-call options win", async () => {
    const clock = new FakeClock();
    const s = await open(clock);
    const first = await s.enqueue({ queue: "q", idempotencyKey: "k", payload: 1, maxAttempts: 1, leaseMs: 1_234, delayMs: 50 });
    const again = await s.enqueue({ queue: "q", idempotencyKey: "k", payload: 1, maxAttempts: 9, leaseMs: 99, runAt: 0 });
    expect(again).toMatchObject({ id: first.id, maxAttempts: 1, leaseMs: 1_234, runAt: first.runAt });
    clock.advance(50);
    const held = await s.claim("q", "w");
    await s.fail(first.id, held!.leaseToken!, "nope");
    expect((await s.enqueue({ queue: "q", idempotencyKey: "k", payload: 1 })).status).toBe("dead");
    await s.requeueDead(first.id);
    const again2 = await s.claim("q", "w");
    await s.complete(first.id, again2!.leaseToken!);
    const done = await s.enqueue({ queue: "q", idempotencyKey: "k", payload: 1 });
    expect(done).toMatchObject({ id: first.id, status: "completed" });
    expect((await s.stats("q"))[0]).toMatchObject({ completed: 1, ready: 0 });
  });

  it("S16: top-level scalar, null, and array payloads round-trip", async () => {
    const s = await open();
    for (const payload of [null, 0, "", "text", true, [], [1, [2, [3]]], { "": { "": null } }]) {
      const job = await s.enqueue({ queue: "q", payload });
      expect((await s.get(job.id))?.payload).toEqual(payload);
    }
  });
});

// Strings JSON can carry but PostgreSQL's TEXT/JSONB types reject. SQLite stores
// them fine, so these are where the backends could diverge (guarantee S19).
describe.each(BACKENDS)("$name: payload/text parity", (backend) => {
  let opened: Opened | undefined;
  afterEach(async () => {
    await opened?.cleanup();
    opened = undefined;
  });

  knownBug(18, backend.name, ["postgres"])("S16/S19: a payload string containing U+0000 round-trips", async () => {
    opened = await backend.open();
    const payload = { blob: "a\u0000b" };
    const job = await opened.storage.enqueue({ queue: "q", payload });
    expect((await opened.storage.get(job.id))?.payload).toEqual(payload);
  });

  knownBug(18, backend.name, ["postgres"])("S16/S19: a payload string with a lone surrogate (valid JSON.stringify output) round-trips", async () => {
    opened = await backend.open();
    const payload = { s: "\uD800" };
    const job = await opened.storage.enqueue({ queue: "q", payload });
    expect((await opened.storage.get(job.id))?.payload).toEqual(payload);
  });

  knownBug(18, backend.name, ["postgres"])("S9/S19: fail() records a handler error message that contains U+0000", async () => {
    opened = await backend.open();
    const s = opened.storage;
    const { id } = await s.enqueue({ queue: "q", payload: 1 });
    const held = await s.claim("q", "w");
    // e.g. an error thrown while parsing a binary upload, quoting the bad bytes
    const failed = await s.fail(id, held!.leaseToken!, "unexpected byte \u0000 at offset 3");
    expect(failed.status).toBe("pending");
  });
});

describe.each(BACKENDS)("$name: retry backoff produces a storable runAt", (backend) => {
  let opened: Opened | undefined;
  afterEach(async () => {
    await opened?.cleanup();
    opened = undefined;
  });

  knownBug(22, backend.name)("B1/S9: jitter off with a fractional factor (1.5) still lets fail() schedule a retry", async () => {
    const clock = new FakeClock();
    opened = await backend.open({ now: clock.now, retry: { baseDelayMs: 1_001, factor: 1.5, maxDelayMs: 60_000, jitter: false } });
    const s = opened.storage;
    const { id } = await s.enqueue({ queue: "q", payload: 1, maxAttempts: 5 });
    const first = await s.claim("q", "w");
    clock.t = (await s.fail(id, first!.leaseToken!, "1")).runAt;
    const retry = await s.claim("q", "w");
    const second = await s.fail(id, retry!.leaseToken!, "2"); // 1001 * 1.5 = 1501.5 ms
    expect(second.status).toBe("pending");
    expect(Number.isInteger(second.runAt)).toBe(true);
  });

  knownBug(23, backend.name)("B1/S9: baseDelayMs 0 with many attempts (factor^n overflows to Infinity) still retries immediately", async () => {
    const clock = new FakeClock();
    opened = await backend.open({ now: clock.now, retry: { baseDelayMs: 0, factor: 2, maxDelayMs: 0, jitter: false } });
    const s = opened.storage;
    const { id } = await s.enqueue({ queue: "q", payload: 1, maxAttempts: 1_100 });
    let last;
    for (let i = 0; i < 1_030; i++) {
      const held = await s.claim("q", "w");
      last = await s.fail(id, held!.leaseToken!, String(i));
    }
    expect(last).toMatchObject({ status: "pending", runAt: clock.t, attempts: 1_030 });
  }, 60_000);
});
