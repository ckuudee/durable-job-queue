import { randomUUID } from "node:crypto";
import { EventEmitter } from "node:events";
import { hostname } from "node:os";
import { JobStateError, type Storage } from "./storage.js";
import type { Job } from "./types.js";

/**
 * Lifecycle events a Worker emits, for metrics and logging. Every `job:claimed`
 * is followed by exactly one of `job:completed`, `job:failed` or
 * `job:abandoned`. In each event, `job` is the job as it was claimed.
 *
 * Events describe only what this Worker did. A job whose lease lapses on its
 * final attempt is marked dead inside some later `claim()`, so no Worker emits
 * an event for it; `Storage.stats()` counts it.
 *
 * There is deliberately no "error" event: EventEmitter throws when "error" is
 * emitted with no listener. Errors the worker survives go to `onError`.
 */
export interface WorkerEvents {
  "job:claimed": [JobClaimedEvent];
  /** Emitted after the completion is recorded in storage. */
  "job:completed": [JobCompletedEvent];
  /** The handler threw, and the failure is recorded. `willRetry` is false when the job went to the dead-letter queue. */
  "job:failed": [JobFailedEvent];
  /**
   * The worker gave the job up without recording a result: its lease was lost
   * to another worker, stop() timed out, or storage rejected the result. The
   * job's lease lapses and it runs again elsewhere (or goes dead if out of attempts).
   */
  "job:abandoned": [JobAbandonedEvent];
}

export interface JobClaimedEvent {
  job: Job;
}
export interface JobCompletedEvent {
  job: Job;
  /** How long the handler ran, in milliseconds. */
  durationMs: number;
}
export interface JobFailedEvent {
  job: Job;
  /** What the handler threw. */
  error: unknown;
  durationMs: number;
  willRetry: boolean;
}
export interface JobAbandonedEvent {
  job: Job;
  reason: unknown;
}

export interface JobContext {
  /**
   * Aborted when the worker loses the job (its lease lapsed and another worker
   * took it) or when stop() times out. Long-running handlers should pass it to
   * fetch() and similar APIs, or check it between steps, and stop early: the
   * job's result will not be recorded, and it will run again elsewhere.
   */
  signal: AbortSignal;
  workerId: string;
}

/** Resolve to complete the job; throw (or reject) to fail it, which retries with backoff. */
export type JobHandler = (job: Job, context: JobContext) => Promise<void>;

export interface WorkerOptions {
  queue: string;
  handler: JobHandler;
  /** Jobs run at the same time by this worker. Defaults to 1. */
  concurrency?: number;
  /** Identifies this worker in the database. Defaults to `<hostname>:<pid>:<random>`. */
  workerId?: string;
  /** How long to wait before claiming again when the queue is empty, in ms. Defaults to 1000. */
  pollIntervalMs?: number;
  /**
   * Called with errors the worker survives: a failed claim (database down), a
   * failed heartbeat, a lost job (JobStateError), or an event listener that
   * threw. Handler errors are not reported here; they fail the job. Defaults
   * to console.error.
   */
  onError?: (err: unknown) => void;
}

export interface StopOptions {
  /**
   * Stop waiting for in-flight jobs after this many ms. Their handlers' signals
   * are aborted and their results are not recorded; their leases lapse and
   * another worker runs them again. Without it, stop() waits as long as it takes.
   */
  timeoutMs?: number;
}

function assertPositiveInteger(name: string, value: number): void {
  if (!Number.isSafeInteger(value) || value < 1) throw new RangeError(`${name} must be a positive integer`);
}

/**
 * Runs a handler for every job in a queue: claims jobs, keeps their leases
 * alive with heartbeats while the handler runs, then completes or fails them.
 *
 * ```ts
 * const worker = new Worker(storage, { queue: "email", handler: async (job) => send(job.payload) });
 * worker.start();
 * process.on("SIGTERM", () => worker.stop({ timeoutMs: 30_000 }));
 * worker.on("job:completed", ({ job, durationMs }) => metrics.observe(job.queue, durationMs));
 * ```
 */
export class Worker extends EventEmitter<WorkerEvents> {
  readonly workerId: string;
  private readonly storage: Storage;
  private readonly queue: string;
  private readonly handler: JobHandler;
  private readonly concurrency: number;
  private readonly pollIntervalMs: number;
  private readonly onError: (err: unknown) => void;

  /** Bumped by stop(), so loops from a previous start() exit even if a later start() runs. */
  private generation = 0;
  private running = false;
  private loops: Promise<void>[] = [];
  private readonly inFlight = new Set<AbortController>();
  private readonly sleepers = new Set<() => void>();

  constructor(storage: Storage, options: WorkerOptions) {
    const {
      queue,
      handler,
      concurrency = 1,
      workerId = `${hostname()}:${process.pid}:${randomUUID().slice(0, 8)}`,
      pollIntervalMs = 1_000,
      onError = (err) => console.error(err),
    } = options;
    super();
    if (queue.trim() === "") throw new RangeError("queue must be a non-empty string");
    if (workerId.trim() === "") throw new RangeError("workerId must be a non-empty string");
    assertPositiveInteger("concurrency", concurrency);
    assertPositiveInteger("pollIntervalMs", pollIntervalMs);
    this.storage = storage;
    this.queue = queue;
    this.handler = handler;
    this.concurrency = concurrency;
    this.workerId = workerId;
    this.pollIntervalMs = pollIntervalMs;
    this.onError = onError;
  }

  /** Begin processing jobs. Calling it again while running does nothing. */
  start(): void {
    if (this.running) return;
    this.running = true;
    const generation = this.generation;
    this.loops = Array.from({ length: this.concurrency }, () => this.loop(generation));
  }

  /**
   * Stop claiming new jobs and wait for the ones in progress to finish. Safe
   * to call when not running.
   */
  async stop({ timeoutMs }: StopOptions = {}): Promise<void> {
    if (timeoutMs !== undefined && (!Number.isSafeInteger(timeoutMs) || timeoutMs < 0)) {
      throw new RangeError("timeoutMs must be a non-negative integer");
    }
    if (!this.running) return;
    this.running = false;
    this.generation++;
    for (const wake of this.sleepers) wake(); // idle loops exit now, not after their poll delay

    const finished = Promise.all(this.loops);
    this.loops = [];
    if (timeoutMs === undefined) {
      await finished;
      return;
    }
    let timer: NodeJS.Timeout | undefined;
    const timedOut = await Promise.race([
      finished.then(() => false),
      new Promise<boolean>((resolve) => {
        timer = setTimeout(() => resolve(true), timeoutMs);
      }),
    ]);
    clearTimeout(timer);
    if (timedOut) {
      for (const controller of this.inFlight) {
        controller.abort(new Error("worker stopped before the job finished"));
      }
    }
  }

  private async loop(generation: number): Promise<void> {
    while (generation === this.generation) {
      let job: Job | null;
      try {
        job = await this.storage.claim(this.queue, this.workerId);
      } catch (err) {
        this.onError(err);
        await this.sleep(this.pollIntervalMs);
        continue;
      }
      // A job claimed just as stop() was called is still ours: run it.
      if (job) {
        this.emitSafely("job:claimed", { job });
        await this.run(job);
      } else {
        await this.sleep(this.pollIntervalMs);
      }
    }
  }

  private async run(job: Job): Promise<void> {
    const controller = new AbortController();
    this.inFlight.add(controller);

    // The job is given up the moment it's aborted (lost lease, or stop() timing
    // out), even if the handler never returns. Removed below before the result
    // is recorded, so a late abort can't announce a job that then completes.
    const onAbort = (): void => this.emitSafely("job:abandoned", { job, reason: controller.signal.reason });
    controller.signal.addEventListener("abort", onAbort, { once: true });
    const startedAt = performance.now();

    const heartbeat = setInterval(() => {
      this.storage.heartbeat(job.id, this.workerId).catch((err: unknown) => {
        if (err instanceof JobStateError) {
          clearInterval(heartbeat);
          controller.abort(err);
        }
        // Anything else (say, a network blip) is reported and retried on the next beat.
        this.onError(err);
      });
    }, Math.max(1, Math.floor(job.leaseMs / 3)));

    try {
      let failure: { error: unknown } | undefined;
      try {
        await this.handler(job, { signal: controller.signal, workerId: this.workerId });
      } catch (error) {
        failure = { error };
      }
      // The job was lost, or stop() gave up on it: it's no longer ours to record
      // (and onAbort has already reported it abandoned).
      if (controller.signal.aborted) return;
      controller.signal.removeEventListener("abort", onAbort);
      clearInterval(heartbeat);
      const durationMs = performance.now() - startedAt;

      try {
        if (failure) {
          const { error } = failure;
          const failed = await this.storage.fail(
            job.id, this.workerId, error instanceof Error ? error.message : String(error),
          );
          this.emitSafely("job:failed", { job, error, durationMs, willRetry: failed.status === "pending" });
        } else {
          await this.storage.complete(job.id, this.workerId);
          this.emitSafely("job:completed", { job, durationMs });
        }
      } catch (err) {
        // complete() or fail() threw: the job was lost (JobStateError) or the
        // database is unreachable. Either way the lease lapses and it runs again.
        this.onError(err);
        this.emitSafely("job:abandoned", { job, reason: err });
      }
    } finally {
      clearInterval(heartbeat);
      this.inFlight.delete(controller);
    }
  }

  /**
   * emit(), but a listener can't hurt the worker. Plain emit() runs listeners
   * synchronously, so a throwing listener would throw into run() and could
   * leave a finished job unrecorded; it would also skip the listeners after it.
   * And an async listener's rejected promise would be an unhandled rejection,
   * which crashes Node by default. Each listener's error goes to onError instead.
   */
  private emitSafely<K extends keyof WorkerEvents>(event: K, ...args: WorkerEvents[K]): void {
    // listeners() returns a copy, so a listener that removes itself (once()) is safe.
    for (const listener of this.listeners(event) as Array<(...a: WorkerEvents[K]) => unknown>) {
      try {
        const result = listener.apply(this, args);
        if (result instanceof Promise) result.catch((err: unknown) => this.onError(err));
      } catch (err) {
        this.onError(err);
      }
    }
  }

  /** setTimeout as a promise that stop() can cut short. */
  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => {
      const wake = (): void => {
        clearTimeout(timer);
        this.sleepers.delete(wake);
        resolve();
      };
      const timer = setTimeout(wake, ms);
      this.sleepers.add(wake);
    });
  }
}
