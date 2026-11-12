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
  private running: Promise<void> | undefined;
  private stopped = true;

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
    this.schedules.set(name, { ...job, timezone });
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

    const loop = (): void => {
      this.running = this.tick()
        .then(() => undefined, this.onError)
        .finally(() => {
          this.running = undefined;
          if (!this.stopped) this.timer = setTimeout(loop, intervalMs);
        });
    };
    loop();
  }

  /** Stop ticking. Resolves once any in-flight tick has finished. */
  async stop(): Promise<void> {
    this.stopped = true;
    clearTimeout(this.timer);
    await this.running;
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
