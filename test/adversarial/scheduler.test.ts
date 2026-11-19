// Cron scheduling: duplicate schedulers, missed ticks, DST, payload changes,
// and the start()/stop() lifecycle ("ticks never overlap").
import { afterEach, describe, expect, it } from "vitest";
import { Scheduler, type Storage } from "../../src/index.js";
import { BACKENDS, FakeClock, sleep, type Opened } from "./helpers/backends.js";
import { knownBug } from "./helpers/known-bug.js";

const HOUR = 3_600_000;
const DAY = 24 * HOUR;

/** Local calendar date of an instant in `timeZone`, e.g. "2026-03-08". */
const localDate = (ms: number, timeZone: string) =>
  new Intl.DateTimeFormat("en-CA", { timeZone, year: "numeric", month: "2-digit", day: "2-digit" }).format(ms);

describe.each(BACKENDS)("$name: Scheduler", (backend) => {
  let opened: Opened | undefined;
  const schedulers: Scheduler[] = [];
  afterEach(async () => {
    for (const sc of schedulers.splice(0)) await sc.stop();
    await opened?.cleanup();
    opened = undefined;
  });

  describe.each([
    { label: "spring forward (2:30 does not exist on 2026-03-08)", cron: "30 2 * * *", from: Date.UTC(2026, 2, 6, 12), days: 4, bug: undefined },
    { label: "fall back (1:30 happens twice on 2026-11-01)", cron: "30 1 * * *", from: Date.UTC(2026, 9, 30, 12), days: 4, bug: undefined },
  ])("SC6: daily job in America/New_York across $label", ({ cron, from, days, bug }) => {
    (bug ? knownBug(bug, backend.name) : it)("runs exactly once per local calendar day", async () => {
      const clock = new FakeClock(from);
      opened = await backend.open({ now: clock.now });
      const sc = new Scheduler(opened.storage, { now: clock.now, lookaheadMs: 2 * HOUR })
        .add({ name: "dst", cron, timezone: "America/New_York", queue: "q", payload: 1 });
      const runAts = new Set<number>();
      for (let t = from; t < from + days * DAY; t += 10 * 60_000) {
        clock.t = t;
        for (const job of await sc.tick()) runAts.add(job.runAt);
      }
      const perDay = new Map<string, string[]>();
      for (const r of [...runAts].sort()) {
        const d = localDate(r, "America/New_York");
        perDay.set(d, [...(perDay.get(d) ?? []), new Date(r).toISOString()]);
      }
      expect([...perDay.values()].filter((v) => v.length !== 1)).toEqual([]);
      expect(perDay.size).toBeGreaterThanOrEqual(days);
    }, 30_000);
  });

  knownBug(20, backend.name)("SC7: stop() followed immediately by start() never runs two ticks at once, and a later stop() really stops", async () => {
    opened = await backend.open();
    const inner = opened.storage;
    let inFlight = 0;
    let peak = 0;
    let calls = 0;
    let gate: Promise<void> = new Promise((r) => setTimeout(r, 80)); // first tick is slow
    const slow: Storage = Object.assign(Object.create(inner) as Storage, {
      enqueue: async (input: Parameters<Storage["enqueue"]>[0]) => {
        calls++;
        peak = Math.max(peak, ++inFlight);
        try {
          await gate;
          return await inner.enqueue(input);
        } finally {
          inFlight--;
        }
      },
    });
    const sc = new Scheduler(slow, { lookaheadMs: 1_000 }).add({ name: "s", cron: "0 0 1 1 *", queue: "q", payload: 1 });
    schedulers.push(sc);
    sc.start(20);
    await sleep(10); // first tick is now blocked inside enqueue
    void sc.stop(); // e.g. a config reload: stop, then start again, without awaiting
    sc.start(20);
    await sleep(150);
    gate = Promise.resolve();
    await sleep(100);
    expect.soft(peak, "ticks in flight at once").toBe(1);

    await sc.stop();
    const after = calls;
    await sleep(100);
    expect.soft(calls - after, "enqueue calls after stop() resolved").toBe(0);
  });
});
