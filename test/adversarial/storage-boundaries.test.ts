// Clock and lease boundaries (off-by-one at runAt / leaseUntil), claim order,
// redelivery accounting, and stats() agreeing with claim(). Guarantee IDs refer
// to test/adversarial/GUARANTEES.md.
import { afterEach, describe, expect, it } from "vitest";
import { JobStateError } from "../../src/index.js";
import { BACKENDS, FakeClock, type Opened } from "./helpers/backends.js";

describe.each(BACKENDS)("$name: boundaries", (backend) => {
  let opened: Opened | undefined;
  afterEach(async () => {
    await opened?.cleanup();
    opened = undefined;
  });

  async function open(clock: FakeClock, extra: { leaseMs?: number } = {}) {
    opened = await backend.open({ now: clock.now, random: () => 0.5, ...extra });
    return opened.storage;
  }

  it("S3: a job is claimable exactly at runAt, not 1 ms before (and stats agree)", async () => {
    const clock = new FakeClock();
    const s = await open(clock);
    const job = await s.enqueue({ queue: "q", payload: 1, delayMs: 1_000 });
    expect(job.runAt).toBe(clock.t + 1_000);

    clock.advance(999);
    expect((await s.stats("q"))[0]).toMatchObject({ ready: 0, scheduled: 1, oldestReadyAgeMs: null });
    expect(await s.claim("q", "w")).toBeNull();

    clock.advance(1);
    expect((await s.stats("q"))[0]).toMatchObject({ ready: 1, scheduled: 0, oldestReadyAgeMs: 0 });
    expect((await s.claim("q", "w"))?.id).toBe(job.id);
  });

  it("S4/ST3: at leaseUntil - 1 the job is running and unclaimable; at leaseUntil it is stalled and claim takes it", async () => {
    const clock = new FakeClock();
    const s = await open(clock);
    await s.enqueue({ queue: "q", payload: 1, leaseMs: 500 });
    const first = await s.claim("q", "a");
    expect(first?.leaseUntil).toBe(clock.t + 500);

    clock.advance(499);
    expect((await s.stats("q"))[0]).toMatchObject({ running: 1, stalled: 0 });
    expect(await s.claim("q", "b")).toBeNull();

    clock.advance(1);
    expect((await s.stats("q"))[0]).toMatchObject({ running: 0, stalled: 1 });
    const second = await s.claim("q", "b");
    expect(second).toMatchObject({ id: first?.id, attempts: 2, workerId: "b" });
    // S8: the original holder lost it.
    await expect(s.complete(first!.id, "a")).rejects.toBeInstanceOf(JobStateError);
    await expect(s.fail(first!.id, "a", "x")).rejects.toBeInstanceOf(JobStateError);
    await expect(s.heartbeat(first!.id, "a")).rejects.toBeInstanceOf(JobStateError);
  });

  it("S8: a holder whose lease lapsed can still complete if nobody re-claimed", async () => {
    const clock = new FakeClock();
    const s = await open(clock);
    const { id } = await s.enqueue({ queue: "q", payload: 1, leaseMs: 100 });
    await s.claim("q", "a");
    clock.advance(10_000);
    await s.complete(id, "a");
    expect((await s.get(id))?.status).toBe("completed");
  });

  it("S7: heartbeat extends from *now* by the job's leaseMs, not the store default", async () => {
    const clock = new FakeClock();
    const s = await open(clock, { leaseMs: 50_000 });
    const { id } = await s.enqueue({ queue: "q", payload: 1, leaseMs: 300 });
    await s.claim("q", "a");
    clock.advance(200);
    const beat = await s.heartbeat(id, "a");
    expect(beat.leaseUntil).toBe(clock.t + 300);
  });

  it("S5/S6/T17: a job abandoned on every attempt goes dead after exactly maxAttempts claims", async () => {
    const clock = new FakeClock();
    const s = await open(clock);
    const { id } = await s.enqueue({ queue: "q", payload: 1, maxAttempts: 3, leaseMs: 100 });
    const attempts: number[] = [];
    for (let i = 0; i < 5; i++) {
      const job = await s.claim("q", `w${i}`);
      if (job) attempts.push(job.attempts);
      clock.advance(100); // lapse
    }
    expect(attempts).toEqual([1, 2, 3]);
    const dead = await s.get(id);
    expect(dead).toMatchObject({ status: "dead", attempts: 3, leaseUntil: null });
    expect((await s.listDead("q")).map((j) => j.id)).toEqual([id]);
    expect((await s.stats("q"))[0]).toMatchObject({ stalled: 0, dead: 1 });
  });

  it("ST3: a stalled job on its final attempt is counted stalled, and claim marks it dead rather than returning it", async () => {
    const clock = new FakeClock();
    const s = await open(clock);
    await s.enqueue({ queue: "q", payload: 1, maxAttempts: 1, leaseMs: 100 });
    await s.claim("q", "a");
    clock.advance(100);
    expect((await s.stats("q"))[0]).toMatchObject({ stalled: 1, dead: 0 });
    expect(await s.claim("q", "b")).toBeNull();
    expect((await s.stats("q"))[0]).toMatchObject({ stalled: 0, dead: 1 });
  });

  it("S2: claim returns the oldest ready job first (by runAt), and FIFO for equal runAt", async () => {
    const clock = new FakeClock();
    const s = await open(clock);
    const late = await s.enqueue({ queue: "q", payload: "late", runAt: clock.t - 10 });
    const early = await s.enqueue({ queue: "q", payload: "early", runAt: clock.t - 1_000 });
    const a = await s.enqueue({ queue: "q", payload: "a" });
    const b = await s.enqueue({ queue: "q", payload: "b" });
    const order: string[] = [];
    for (;;) {
      const j = await s.claim("q", "w");
      if (!j) break;
      order.push(j.id);
    }
    expect(order).toEqual([early.id, late.id, a.id, b.id]);
  });

  it("S9/B1: retry runAt follows base·factor^(n-1) without jitter and is capped", async () => {
    const clock = new FakeClock();
    opened = await backend.open({ now: clock.now, retry: { baseDelayMs: 100, factor: 3, maxDelayMs: 1_000, jitter: false } });
    const s = opened.storage;
    const { id } = await s.enqueue({ queue: "q", payload: 1, maxAttempts: 5 });
    const delays: number[] = [];
    for (let i = 0; i < 4; i++) {
      await s.claim("q", "w");
      const failed = await s.fail(id, "w", `boom ${i}`);
      expect(failed.status).toBe("pending");
      delays.push(failed.runAt - clock.t);
      clock.t = failed.runAt;
    }
    expect(delays).toEqual([100, 300, 900, 1_000]);
    await s.claim("q", "w");
    expect((await s.fail(id, "w", "last")).status).toBe("dead");
  });

  it("B2: with jitter and random() = 0 the retry still waits at least delay/2", async () => {
    const clock = new FakeClock();
    opened = await backend.open({ now: clock.now, random: () => 0, retry: { baseDelayMs: 1_000, jitter: true } });
    const s = opened.storage;
    const { id } = await s.enqueue({ queue: "q", payload: 1 });
    await s.claim("q", "w");
    const failed = await s.fail(id, "w", "x");
    expect(failed.runAt - clock.t).toBe(500);
    clock.advance(499);
    expect(await s.claim("q", "w")).toBeNull();
  });

  it("S11: requeueDead gives a fresh attempt count and makes the job ready now; non-dead jobs are refused", async () => {
    const clock = new FakeClock();
    const s = await open(clock);
    const { id } = await s.enqueue({ queue: "q", payload: 1, maxAttempts: 1 });
    await s.claim("q", "w");
    await s.fail(id, "w", "bad");
    await expect(s.requeueDead("no-such-id")).rejects.toBeInstanceOf(JobStateError);
    clock.advance(5_000);
    const requeued = await s.requeueDead(id);
    expect(requeued).toMatchObject({ status: "pending", attempts: 0, runAt: clock.t, leaseUntil: null });
    await expect(s.requeueDead(id)).rejects.toBeInstanceOf(JobStateError);
    expect((await s.claim("q", "w"))?.attempts).toBe(1);
  });

  it("S10: listDead is oldest failure first and honours limit", async () => {
    const clock = new FakeClock();
    const s = await open(clock);
    const ids: string[] = [];
    for (let i = 0; i < 3; i++) ids.push((await s.enqueue({ queue: "q", payload: i, maxAttempts: 1 })).id);
    for (let i = 0; i < 3; i++) await s.claim("q", `w${i}`);
    // Fail them in reverse enqueue order, so failure order != creation order.
    const reversed = [...ids].reverse();
    for (const [i, id] of reversed.entries()) {
      clock.advance(10);
      await s.fail(id, `w${ids.indexOf(id)}`, `x${i}`);
    }
    expect((await s.listDead("q")).map((j) => j.id)).toEqual(reversed);
    expect((await s.listDead("q", 2)).map((j) => j.id)).toEqual(reversed.slice(0, 2));
  });

  it("ST2: stats() is sorted by queue name identically on every backend, and an unknown queue gives []", async () => {
    const clock = new FakeClock();
    const s = await open(clock);
    const names = ["b", "a", "Z", "é", "\u{1F600}", "�", "a b", "A"];
    for (const queue of names) await s.enqueue({ queue, payload: 1 });
    const listed = (await s.stats()).map((r) => r.queue);
    // Byte order of the UTF-8 encoding, which is what both backends document.
    const byBytes = [...names].sort((x, y) => Buffer.compare(Buffer.from(x), Buffer.from(y)));
    expect(listed).toEqual(byBytes);
    expect(await s.stats("nope")).toEqual([]);
  });
});
