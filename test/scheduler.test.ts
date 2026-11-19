import { describe, expect, it } from "vitest";
import { Scheduler, SqliteStorage, type RecurringJob, type Storage } from "../src/index.js";

const DAY = 86_400_000;
const MIN = 60_000;
// 2026-01-15T00:00:00Z, a Thursday in winter (no daylight saving in New York).
const JAN_15 = Date.UTC(2026, 0, 15);

const DAILY_9AM: RecurringJob = { name: "report", cron: "0 9 * * *", queue: "q", payload: { kind: "report" } };

/** A store and schedulers that all read the same controllable clock. */
function setup(start = JAN_15 + 8 * 60 * MIN) {
  const clock = { t: start };
  const now = () => clock.t;
  const storage = new SqliteStorage({ path: ":memory:", now });
  const scheduler = (opts: { lookaheadMs?: number } = {}) => new Scheduler(storage, { now, ...opts });
  return { clock, storage, scheduler };
}

async function drain(storage: Storage, queue = "q"): Promise<string[]> {
  const ids: string[] = [];
  for (let job = await storage.claim(queue, "w"); job; job = await storage.claim(queue, "w")) {
    ids.push(job.id);
    await storage.complete(job.id, job.leaseToken!);
  }
  return ids;
}

describe("Scheduler", () => {
  it("enqueues the next occurrence as a delayed job with a per-occurrence key", async () => {
    const { storage, scheduler } = setup();
    const s = scheduler().add({ ...DAILY_9AM, maxAttempts: 5, leaseMs: 2_000 });

    const [job, ...rest] = await s.tick();
    const nineAm = JAN_15 + 9 * 60 * MIN;
    expect(rest).toEqual([]);
    expect(job).toMatchObject({
      queue: "q",
      payload: { kind: "report" },
      status: "pending",
      runAt: nineAm,
      idempotencyKey: `cron:report:${nineAm}`,
      maxAttempts: 5,
      leaseMs: 2_000,
    });
    await storage.close();
  });

  it("doesn't run the job before its time, and schedules the following one after", async () => {
    const { clock, storage, scheduler } = setup();
    const s = scheduler().add(DAILY_9AM);
    await s.tick();

    clock.t = JAN_15 + 9 * 60 * MIN - 1;
    expect(await storage.claim("q", "w")).toBeNull();
    clock.t += 1;
    expect(await drain(storage)).toHaveLength(1);

    const [next] = await s.tick();
    expect(next?.runAt).toBe(JAN_15 + DAY + 9 * 60 * MIN);
    await storage.close();
  });

  it("never duplicates an occurrence, however often or from however many schedulers it ticks", async () => {
    const { clock, storage, scheduler } = setup();
    const a = scheduler().add(DAILY_9AM);
    const b = scheduler().add(DAILY_9AM);

    const results = await Promise.all([a.tick(), b.tick(), a.tick(), b.tick()]);
    expect(new Set(results.flat().map((j) => j.id)).size).toBe(1);

    clock.t = JAN_15 + 9 * 60 * MIN;
    expect(await drain(storage)).toHaveLength(1);
    await storage.close();
  });

  it("enqueues every occurrence inside the lookahead window", async () => {
    const { storage, scheduler } = setup(JAN_15 + 30_000); // 00:00:30
    const s = scheduler({ lookaheadMs: 3 * MIN }).add({ ...DAILY_9AM, cron: "* * * * *" });

    const jobs = await s.tick();
    // Window ends at 00:03:30, so 00:01, 00:02 and 00:03.
    expect(jobs.map((j) => j.runAt)).toEqual([JAN_15 + MIN, JAN_15 + 2 * MIN, JAN_15 + 3 * MIN]);
    await storage.close();
  });

  it("supports a leading seconds field", async () => {
    const { storage, scheduler } = setup(JAN_15);
    const s = scheduler({ lookaheadMs: 1_000 }).add({ ...DAILY_9AM, cron: "*/30 * * * * *" });
    expect((await s.tick()).map((j) => j.runAt)).toEqual([JAN_15 + 30_000]);
    await storage.close();
  });

  it("runs the queued occurrence late after downtime, but doesn't fire every missed one", async () => {
    const { clock, storage, scheduler } = setup();
    const s = scheduler().add(DAILY_9AM);
    await s.tick(); // queues Jan 15, 09:00

    clock.t = JAN_15 + 5 * DAY + 12 * 60 * MIN; // down until Jan 20, 12:00
    const [next] = await s.tick();
    expect(next?.runAt).toBe(JAN_15 + 6 * DAY + 9 * 60 * MIN); // Jan 21, not Jan 16..20

    expect(await drain(storage)).toHaveLength(1); // only the Jan 15 job, run late
    await storage.close();
  });

  it("evaluates the expression in the schedule's time zone", async () => {
    const { storage, scheduler } = setup(JAN_15);
    const s = scheduler().add({ ...DAILY_9AM, timezone: "America/New_York" });
    const [job] = await s.tick();
    expect(job?.runAt).toBe(JAN_15 + 14 * 60 * MIN); // 09:00 EST is 14:00 UTC
    await storage.close();
  });

  // #19. Ticks every 10 minutes through the night clocks fall back, so some tick
  // lands inside the repeated hour, and collects every occurrence enqueued.
  describe("the day clocks fall back", () => {
    const HOUR = 60 * MIN;
    async function runAtsAcross(cron: string, timezone: string, from: number, hours: number): Promise<string[]> {
      const { clock, storage, scheduler } = setup(from);
      const s = scheduler({ lookaheadMs: 2 * HOUR }).add({ name: "dst", cron, timezone, queue: "q", payload: 1 });
      const runAts = new Set<number>();
      for (; clock.t < from + hours * HOUR; clock.t += 10 * MIN) {
        for (const job of await s.tick()) runAts.add(job.runAt);
      }
      await storage.close();
      return [...runAts].sort((a, b) => a - b).map((t) => new Date(t).toISOString());
    }
    // New York, 2026-11-01: 02:00 EDT (06:00Z) becomes 01:00 EST, so 01:00-01:59 happens twice.
    const NOV_1_NY = Date.UTC(2026, 10, 1);

    it("runs a fixed-time job once, at the first of the two repeated local times", async () => {
      expect(await runAtsAcross("30 1 * * *", "America/New_York", NOV_1_NY, 12)).toEqual([
        "2026-11-01T05:30:00.000Z", // 01:30 EDT; 01:30 EST (06:30Z) is skipped
        "2026-11-02T06:30:00.000Z", // the next day's 01:30 EST, always queued ahead
      ]);
    });

    it("runs a job that fires every hour in both passes, since they are different hours", async () => {
      const runAts = await runAtsAcross("30 * * * *", "America/New_York", NOV_1_NY + 4 * HOUR, 4);
      expect(runAts.slice(0, 3)).toEqual([
        "2026-11-01T04:30:00.000Z", // 00:30 EDT
        "2026-11-01T05:30:00.000Z", // 01:30 EDT
        "2026-11-01T06:30:00.000Z", // 01:30 EST
      ]);
    });

    it("works for a zone whose clocks go back by 30 minutes", async () => {
      // Lord Howe Island, 2026-04-05: 02:00 (+11) becomes 01:30 (+10:30), so 01:30-01:59 repeats.
      expect(await runAtsAcross("45 1 * * *", "Australia/Lord_Howe", Date.UTC(2026, 3, 4, 12), 12)).toEqual([
        "2026-04-04T14:45:00.000Z", // 01:45 +11; 01:45 +10:30 (15:15Z) is skipped
        "2026-04-05T15:15:00.000Z", // the next day's 01:45 +10:30
      ]);
    });
  });

  it("keeps an already-queued occurrence when the payload changes, and uses the new payload after", async () => {
    const { clock, storage, scheduler } = setup();
    await scheduler().add(DAILY_9AM).tick(); // Jan 15 queued with the old payload

    const redeployed = scheduler().add({ ...DAILY_9AM, payload: { kind: "report", v: 2 } });
    expect(await redeployed.tick()).toEqual([]); // no error, nothing new

    clock.t = JAN_15 + 9 * 60 * MIN;
    expect((await storage.claim("q", "w"))?.payload).toEqual({ kind: "report" });
    const [next] = await redeployed.tick();
    expect(next?.payload).toEqual({ kind: "report", v: 2 });
    await storage.close();
  });

  it("rejects bad names, duplicates, invalid expressions and unknown time zones", async () => {
    const { storage, scheduler } = setup();
    const s = scheduler().add(DAILY_9AM);
    expect(() => s.add(DAILY_9AM)).toThrow(/already exists/);
    expect(() => s.add({ ...DAILY_9AM, name: "  " })).toThrow(RangeError);
    expect(() => s.add({ ...DAILY_9AM, name: "x".repeat(201) })).toThrow(RangeError);
    expect(() => s.add({ ...DAILY_9AM, name: "bad-cron", cron: "61 * * * *" })).toThrow(RangeError);
    expect(() => s.add({ ...DAILY_9AM, name: "bad-tz", timezone: "Not/AZone" })).toThrow(RangeError);
    expect(() => new Scheduler(storage, { lookaheadMs: 0 })).toThrow(RangeError);
    await storage.close();
  });

  describe("start and stop", () => {
    it("refuses a tick interval that isn't shorter than the lookahead", async () => {
      const { storage, scheduler } = setup();
      expect(() => scheduler({ lookaheadMs: 1_000 }).start(1_000)).toThrow(/less than lookaheadMs/);
      await storage.close();
    });

    it("ticks in the background until stopped", async () => {
      const { clock, storage, scheduler } = setup();
      const s = scheduler().add(DAILY_9AM);
      s.start(5);
      s.start(5); // a second start is a no-op
      await new Promise((resolve) => setTimeout(resolve, 30));
      await s.stop();

      // Only the background ticks could have queued this; several ran, one job resulted.
      clock.t = JAN_15 + 9 * 60 * MIN;
      expect(await drain(storage)).toHaveLength(1);
      await storage.close();
    });

    it("reports background tick failures to onError and keeps ticking", async () => {
      const failing = {
        enqueue: () => Promise.reject(new Error("db down")),
      } as unknown as Storage;
      const errors: unknown[] = [];
      const s = new Scheduler(failing, { now: () => JAN_15, onError: (e) => errors.push(e) }).add(DAILY_9AM);
      s.start(5);
      await new Promise((resolve) => setTimeout(resolve, 40));
      await s.stop();
      expect(errors.length).toBeGreaterThan(1); // failed, then tried again
      expect((errors[0] as Error).message).toBe("db down");
    });
  });
});
