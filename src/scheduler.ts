import { CronExpressionParser } from "cron-parser";
import { IdempotencyConflictError, type Storage } from "./storage.js";
import type { Job } from "./types.js";

/** Keeps `cron:<name>:<epoch ms>` within the 255-character idempotency key limit. */
export const MAX_SCHEDULE_NAME_LENGTH = 200;

/** Safety cap so a very frequent schedule with a long lookahead can't enqueue without bound. */
const MAX_OCCURRENCES_PER_TICK = 1_000;

export interface RecurringJob {
  /** Unique name for the schedule. Part of each occurrence's idempotency key, so keep it stable. */
  name: string;
  /**
   * Cron expression: 5 fields (minute hour day-of-month month day-of-week), an
   * optional leading seconds field, or a preset like "@daily".
   */
  cron: string;
  /** IANA time zone the expression is evaluated in, e.g. "America/New_York". Defaults to "UTC". */
  timezone?: string;
  queue: string;
  payload: unknown;
  maxAttempts?: number;
  leaseMs?: number;
}

export interface SchedulerOptions {
  /** Injectable clock (epoch ms) so tests control time. */
  now?: () => number;
  /**
   * How far ahead each tick enqueues occurrences, in ms. Must exceed the tick
   * interval, or occurrences between ticks could be missed. Defaults to 60 s.
   */
  lookaheadMs?: number;
  /** Called when a background tick (from start()) fails. Defaults to console.error. */
  onError?: (err: unknown) => void;
}

interface Registered extends RecurringJob {
  timezone: string;
  /** Fires at specific times of day rather than repeatedly through every hour. See isFixedTime. */
  fixedTime: boolean;
}

/** Longer than any daylight-saving shift in the tz database (the largest is 2 h). */
const MAX_DST_SHIFT_MS = 3 * 3_600_000;

const wallClockFormats = new Map<string, Intl.DateTimeFormat>();

/** The zone's UTC offset at instant `t`, in ms (e.g. -4 h for New York in summer). */
function utcOffsetMs(t: number, timeZone: string): number {
  let format = wallClockFormats.get(timeZone);
  if (!format) {
    format = new Intl.DateTimeFormat("en-US", {
      timeZone,
      hourCycle: "h23",
      year: "numeric", month: "numeric", day: "numeric",
      hour: "numeric", minute: "numeric", second: "numeric",
    });
    wallClockFormats.set(timeZone, format);
  }
  const part: Record<string, number> = {};
  for (const { type, value } of format.formatToParts(t)) part[type] = Number(value);
  const wallAsUtc = Date.UTC(part.year!, part.month! - 1, part.day, part.hour, part.minute, part.second);
  return wallAsUtc - (t - (((t % 1000) + 1000) % 1000)); // compare whole seconds
}

/**
 * True when the local wall-clock time at `t` already happened earlier: `t` is
 * in the second pass through the hour that repeats when clocks fall back
 * (1:30 EST, after 1:30 EDT, in New York).
 */
function isRepeatedWallTime(t: number, timeZone: string): boolean {
  const before = utcOffsetMs(t - MAX_DST_SHIFT_MS, timeZone);
  const shift = before - utcOffsetMs(t, timeZone);
  // Clocks went back by `shift` within the window, and the instant that much
  // earlier was still on the old offset, so it showed the same wall time.
  return shift > 0 && utcOffsetMs(t - shift, timeZone) === before;
}

/**
 * A schedule that fires at specific times of day: one second and minute value,
 * and not every hour ("30 1 * * *", "0 9,17 * * 1-5"). On the day clocks fall
 * back it runs once, at the first of the two repeated local times, as classic
 * cron does. Anything that fires every hour or more often ("30 * * * *",
 * "*\/15 1 * * *") follows real time instead and runs in both passes, which
 * really are different hours.
 */
function isFixedTime(cron: string, timezone: string): boolean {
  const { second, minute, hour } = CronExpressionParser.parse(cron, { tz: timezone }).fields;
  return second.values.length === 1 && minute.values.length === 1 && hour.values.length < 24;
}

/**
 * Turns recurring schedules into ordinary delayed jobs. Each tick enqueues
 * every occurrence due within the lookahead window, with `runAt` set to the
 * occurrence time and the idempotency key `cron:<name>:<time>`.
 *
 * The scheduler keeps no state of its own; the idempotency key does the work:
 * - Run as many scheduler processes as you like (one per app instance is
 *   fine). Each occurrence becomes exactly one job, because the database lets
 *   only one insert per key succeed.
 * - Ticking twice, or restarting, never duplicates a job.
 * - After downtime there is no catch-up storm: the occurrence that was already
 *   enqueued still runs (late), but occurrences that fell entirely inside the
 *   outage are skipped rather than all fired at once.
 *
 * Changing a schedule's payload takes effect from the first occurrence not yet
 * enqueued; occurrences already in the queue keep the payload they were given.
 */
export class Scheduler {
  private readonly storage: Storage;
  private readonly now: () => number;
  private readonly lookaheadMs: number;
  private readonly onError: (err: unknown) => void;
  private readonly schedules = new Map<string, Registered>();

  private timer: NodeJS.Timeout | undefined;
  /**
   * The last background tick, from this start() or an earlier one. Each new
   * tick chains onto it, so ticks never overlap even across stop()/start().
   */
  private running: Promise<void> = Promise.resolve();
  private stopped = true;
  /** Bumped by every start() and stop(); a loop keeps going only while its generation is current. */
  private generation = 0;

  constructor(
    storage: Storage,
    { now = Date.now, lookaheadMs = 60_000, onError = (err) => console.error(err) }: SchedulerOptions = {},
  ) {
    if (!Number.isSafeInteger(lookaheadMs) || lookaheadMs < 1) {
      throw new RangeError("lookaheadMs must be a positive integer");
    }
    this.storage = storage;
    this.now = now;
    this.lookaheadMs = lookaheadMs;
    this.onError = onError;
  }

  /** Register a schedule. Throws RangeError for a bad name, cron expression or time zone. */
  add(job: RecurringJob): this {
    const { name, cron, timezone = "UTC" } = job;
    if (name.trim() === "" || name.length > MAX_SCHEDULE_NAME_LENGTH) {
      throw new RangeError(`schedule name must be 1 to ${MAX_SCHEDULE_NAME_LENGTH} characters, not blank`);
    }
    if (this.schedules.has(name)) throw new RangeError(`a schedule named "${name}" already exists`);
    try {
      // Computing one occurrence also validates the time zone, which the parser
      // only checks lazily.
      CronExpressionParser.parse(cron, { currentDate: this.now(), tz: timezone }).next();
    } catch (err) {
      throw new RangeError(`invalid schedule "${name}" (${cron}, ${timezone}): ${(err as Error).message}`);
    }
    this.schedules.set(name, { ...job, timezone, fixedTime: isFixedTime(cron, timezone) });
    return this;
  }

  /**
   * Enqueue every occurrence after now and up to now + lookaheadMs, and always
   * at least the next one, for every schedule. Safe to call any number of
   * times, from any number of processes. Resolves to the queued job for each
   * occurrence, whether this call created it or an earlier tick did.
   */
  async tick(): Promise<Job[]> {
    const now = this.now();
    const jobs: Job[] = [];
    for (const schedule of this.schedules.values()) {
      for (const runAt of this.occurrences(schedule, now)) {
        const job = await this.enqueueOccurrence(schedule, runAt);
        if (job) jobs.push(job);
      }
    }
    return jobs;
  }

  /** Tick now and then every `intervalMs` until stop(). Ticks never overlap. */
  start(intervalMs = 10_000): void {
    if (!Number.isSafeInteger(intervalMs) || intervalMs < 1) {
      throw new RangeError("intervalMs must be a positive integer");
    }
    if (intervalMs >= this.lookaheadMs) {
      throw new RangeError("intervalMs must be less than lookaheadMs, or occurrences between ticks could be missed");
    }
    if (!this.stopped) return;
    this.stopped = false;
    const generation = ++this.generation;

    // A loop from an earlier start() can still be mid-tick (stop() then start()
    // without awaiting). Checking the generation, not a shared flag, keeps that
    // old loop from rescheduling itself and running alongside this one (#20).
    const loop = (): void => {
      this.timer = undefined;
      this.running = this.running.then(async () => {
        if (generation !== this.generation) return;
        try {
          await this.tick();
        } catch (err) {
          this.reportError(err);
        }
        if (generation === this.generation) this.timer = setTimeout(loop, intervalMs);
      });
    };
    loop();
  }

  /** Stop ticking. Resolves once any in-flight tick has finished. */
  async stop(): Promise<void> {
    this.stopped = true;
    this.generation++;
    clearTimeout(this.timer);
    this.timer = undefined;
    await this.running;
  }

  /**
   * Hands a tick failure to onError without letting `running` reject: a
   * rejected link would stop every later tick from chaining onto it. If onError
   * itself throws, that is a bug in the caller's handler, so let it surface as
   * an uncaught exception instead of hiding it.
   */
  private reportError(err: unknown): void {
    try {
      this.onError(err);
    } catch (handlerErr) {
      queueMicrotask(() => {
        throw handlerErr;
      });
    }
  }

  private occurrences(schedule: Registered, now: number): number[] {
    const expression = CronExpressionParser.parse(schedule.cron, {
      currentDate: now,
      tz: schedule.timezone,
    });
    const until = now + this.lookaheadMs;
    const times: number[] = [];
    while (times.length < MAX_OCCURRENCES_PER_TICK) {
      const t = expression.next().getTime();
      if (times.length > 0 && t > until) break;
      // Each tick parses from `now`, so a tick inside the repeated hour would
      // otherwise see the second 1:30 as "next" and enqueue it under a new key:
      // a daily job that runs twice (#19).
      if (schedule.fixedTime && isRepeatedWallTime(t, schedule.timezone)) continue;
      times.push(t);
    }
    return times;
  }

  /** The occurrence's job, or null if it is queued under an older payload. */
  private async enqueueOccurrence(schedule: Registered, runAt: number): Promise<Job | null> {
    try {
      return await this.storage.enqueue({
        queue: schedule.queue,
        payload: schedule.payload,
        maxAttempts: schedule.maxAttempts,
        leaseMs: schedule.leaseMs,
        runAt,
        idempotencyKey: `cron:${schedule.name}:${runAt}`,
      });
    } catch (err) {
      // The occurrence is already queued with an older payload (the schedule
      // changed between deploys). Keep the queued job; later ones get the new payload.
      if (err instanceof IdempotencyConflictError) return null;
      throw err;
    }
  }
}
