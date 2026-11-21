// One function per scenario in docs/design/load-tests.md. Each call is one run
// on a fresh database: set up, run the child processes, read the job rows back,
// check correctness, compute the numbers, and delete the database.
import { setTimeout as sleep } from "node:timers/promises";
import { Child, killAll, startTogether } from "./lib/child.js";
import { distribution, type Distribution } from "./lib/stats.js";
import {
  analyze,
  createTarget,
  destroyTarget,
  explainClaim,
  openStorage,
  ProgressReader,
  QUEUE,
  readRows,
  seedCompleted,
  type Backend,
  type Row,
  type Target,
} from "./lib/target.js";

// Not Math.min(...xs): spreading a huge array into arguments can overflow the stack.
const min = (xs: number[]) => xs.reduce((a, b) => (b < a ? b : a), Infinity);
const max = (xs: number[]) => xs.reduce((a, b) => (b > a ? b : a), -Infinity);

/** A worker layout: `processes` worker processes, each running `concurrency` jobs at a time. */
export interface Layout {
  processes: number;
  concurrency: number;
}

/** What went wrong in a run that broke a guarantee. Recorded in place of a number. */
export interface Facts {
  /** Rows whose `attempts` isn't 1: claimed more than once. */
  claimedMoreThanOnce: number;
  /** Rows whose handler ran some number of times other than once (0, 2, ...). */
  handlerNotOnce: number;
  /** Jobs enqueued that aren't completed. */
  notCompleted: number;
  abandoned: number;
  workerErrors: number;
  firstError: string | null;
}

const NO_FACTS: Facts = {
  claimedMoreThanOnce: 0,
  handlerNotOnce: 0,
  notCompleted: 0,
  abandoned: 0,
  workerErrors: 0,
  firstError: null,
};

/** Thrown when a run breaks a guarantee; the runner reports it instead of a number. */
export class CorrectnessError extends Error {
  constructor(
    readonly problems: string[],
    readonly facts: Facts,
  ) {
    super(`correctness check failed:\n  - ${problems.join("\n  - ")}`);
    this.name = "CorrectnessError";
  }
}

interface WorkerReport {
  abandoned: number;
  errorCount: number;
  errors: string[];
  handled: string[];
}

/**
 * The correctness check every run passes before its numbers count: every job
 * enqueued completed, was claimed exactly once (attempts = 1, since nothing
 * crashes and leases are 30 s) and had its handler run exactly once, and no
 * worker gave a job up or hit an error.
 */
function checkCompleted(rows: Row[], expected: number, workers: WorkerReport[]): void {
  const runs = new Map<string, number>();
  for (const id of workers.flatMap((w) => w.handled)) runs.set(id, (runs.get(id) ?? 0) + 1);
  const errors = workers.flatMap((w) => w.errors);
  const facts: Facts = {
    claimedMoreThanOnce: rows.filter((r) => r.attempts !== 1).length,
    handlerNotOnce: rows.filter((r) => runs.get(r.id) !== 1).length,
    notCompleted: rows.filter((r) => r.status !== "completed").length,
    abandoned: workers.reduce((n, w) => n + w.abandoned, 0),
    workerErrors: workers.reduce((n, w) => n + w.errorCount, 0),
    firstError: errors[0] ?? null,
  };
  const ids = new Set(rows.map((r) => r.id));
  const strangers = [...runs.keys()].filter((id) => !ids.has(id)).length;

  const problems: string[] = [];
  if (rows.length !== expected) problems.push(`expected ${expected} jobs, found ${rows.length}`);
  if (facts.notCompleted > 0) problems.push(`${facts.notCompleted} jobs not completed`);
  if (facts.claimedMoreThanOnce > 0) problems.push(`${facts.claimedMoreThanOnce} jobs claimed more than once`);
  if (facts.handlerNotOnce > 0) problems.push(`${facts.handlerNotOnce} jobs whose handler didn't run exactly once`);
  if (strangers > 0) problems.push(`handler ran ${strangers} jobs that aren't in the table`);
  if (facts.abandoned > 0) problems.push(`${facts.abandoned} jobs abandoned by workers`);
  if (facts.workerErrors > 0) problems.push(`${facts.workerErrors} worker errors, e.g. ${facts.firstError}`);
  if (problems.length > 0) throw new CorrectnessError(problems, facts);
}

function startWorkers(target: Target, layout: Layout, pollIntervalMs: number): Promise<Child[]> {
  return startTogether(
    (i) =>
      new Child("worker-proc.ts", [
        JSON.stringify(target),
        String(layout.concurrency),
        String(pollIntervalMs),
        `bench-worker-${i}`,
      ]),
    layout.processes,
  );
}

async function stopWorkers(workers: Child[]): Promise<WorkerReport[]> {
  const reports = workers.map((w) => w.next("done"));
  for (const w of workers) w.send({ type: "stop" });
  return (await Promise.all(reports)) as unknown as WorkerReport[];
}

/**
 * Wait until no job in the queue is pending or running. Fails if the count
 * doesn't move for `stallMs`, so a hung run ends with an error, not forever.
 */
async function waitForDrained(target: Target, stallMs = 60_000): Promise<void> {
  const progress = await ProgressReader.open(target);
  try {
    let last = -1;
    let lastChange = Date.now();
    for (;;) {
      const unfinished = await progress.unfinished();
      if (unfinished === 0) return;
      if (unfinished !== last) {
        last = unfinished;
        lastChange = Date.now();
      } else if (Date.now() - lastChange > stallMs) {
        throw new Error(`no progress for ${stallMs} ms: ${unfinished} jobs still pending or running`);
      }
      await sleep(100);
    }
  } finally {
    await progress.close();
  }
}

/** Enqueue `count` ready jobs from this process, a few at a time. */
async function preload(target: Target, count: number): Promise<void> {
  const storage = await openStorage(target, 16);
  try {
    let next = 0;
    await Promise.all(
      Array.from({ length: 16 }, async () => {
        while (next < count) await storage.enqueue({ queue: QUEUE, payload: { n: next++ } });
      }),
    );
  } finally {
    await storage.close();
  }
}

/** Databases of runs in progress, so an interrupted runner can delete them. */
const active = new Set<Target>();

/** Kill every child and delete every database still in use. For Ctrl-C and SIGTERM. */
export async function abortAll(): Promise<void> {
  killAll();
  await Promise.allSettled([...active].map((t) => destroyTarget(t)));
}

/** Create a target, run `fn`, and always delete the target and stop stray children. */
async function withTarget<T>(backend: Backend, fn: (target: Target, children: Child[]) => Promise<T>): Promise<T> {
  const target = await createTarget(backend);
  active.add(target);
  const children: Child[] = [];
  const cleanUp = async () => {
    for (const c of children) c.kill();
    active.delete(target);
    await destroyTarget(target);
  };
  let result: T;
  try {
    result = await fn(target, children);
  } catch (err) {
    // Clean up, but don't let a cleanup error hide the error that ended the run.
    await cleanUp().catch((cleanupErr) => console.error(`(cleanup after a failed run also failed: ${cleanupErr})`));
    throw err;
  }
  await cleanUp();
  return result;
}

// --- S1 drain, S4 large tables, S5 backlog curve ------------------------------

export interface CurvePoint {
  /** Jobs still pending at the middle of this window. */
  pending: number;
  jobsPerSecond: number;
}

export interface DrainResult {
  /** The whole drain: jobs ÷ (last completion − first claim). Depends on the preload size. */
  overallJobsPerSecond: number;
  /** While the pending backlog fell from 2 × `lowBacklog` to `lowBacklog`. S1's headline number. */
  lowBacklogJobsPerSecond: number | null;
  /** Throughput against remaining backlog, from the completion times (S5). */
  curve: CurvePoint[];
  /** S4 only: the claim query's plan with the seeded rows present. */
  plan?: string;
}

export interface DrainOptions {
  /** Completed rows inserted before the run (S4). */
  seeded?: number;
  /** S1's measuring window, in pending jobs. Needs `jobs` ≥ 2 × this. */
  lowBacklog?: number;
  /** Completions per curve point. */
  curveWindow?: number;
}

/**
 * Throughput against pending backlog, read from one drain's completion times.
 * Claims follow `ORDER BY run_at, seq` and at most (processes × slots) jobs are
 * in flight, so when the k-th job completes, about N − k are still pending.
 * Windows near the end, where fewer jobs remain than 10× the slot count, are
 * dropped: slots go idle there, so they'd measure the tail, not the backlog.
 */
export function backlogCurve(finishedAt: number[], window: number, slots: number): CurvePoint[] {
  const f = [...finishedAt].sort((a, b) => a - b);
  const n = f.length;
  const points: CurvePoint[] = [];
  for (let k = 0; k + window < n && n - k - window >= 10 * slots; k += window) {
    points.push({
      pending: Math.round(n - k - window / 2),
      jobsPerSecond: (window * 1000) / Math.max(1, f[k + window]! - f[k]!),
    });
  }
  return points;
}

/** Throughput while the pending backlog fell from 2 × `low` to `low`. */
export function lowBacklogRate(finishedAt: number[], low: number): number | null {
  const f = [...finishedAt].sort((a, b) => a - b);
  const n = f.length;
  if (n < 2 * low) return null;
  return (low * 1000) / Math.max(1, f[n - low]! - f[n - 2 * low]!);
}

/**
 * Preload `jobs` ready jobs, start every worker at once, and drain. One run
 * yields the overall rate, the low-backlog rate (S1) and the curve (S5); with
 * `seeded` completed rows already in the table, it is S4.
 */
export function drain(backend: Backend, layout: Layout, jobs: number, options: DrainOptions = {}): Promise<DrainResult> {
  const { seeded = 0, lowBacklog = 1000, curveWindow = 1000 } = options;
  return withTarget(backend, async (target, children) => {
    await seedCompleted(target, seeded);
    await preload(target, jobs);
    await analyze(target); // planner statistics that include the pending jobs
    const plan = seeded > 0 ? await explainClaim(target) : undefined;

    const workers = await startWorkers(target, layout, 1000);
    children.push(...workers);
    await waitForDrained(target);
    const reports = await stopWorkers(workers);

    const rows = await readRows(target);
    checkCompleted(rows, jobs, reports);
    const finishedAt = rows.map((r) => r.finishedAt!);
    return {
      overallJobsPerSecond: (jobs * 1000) / Math.max(1, max(finishedAt) - min(rows.map((r) => r.claimedAt!))),
      lowBacklogJobsPerSecond: lowBacklogRate(finishedAt, lowBacklog),
      curve: backlogCurve(finishedAt, curveWindow, layout.processes * layout.concurrency),
      plan,
    };
  });
}

// --- S2 enqueue ---------------------------------------------------------------

export interface EnqueueResult {
  jobsPerSecond: number;
  /** Duration of each enqueue() call, as the producer saw it. */
  callMs: Distribution;
}

/** S2: `producers` processes enqueue `jobs` jobs between them, back to back. */
export function enqueue(backend: Backend, producers: number, jobs: number, keyed: boolean): Promise<EnqueueResult> {
  return withTarget(backend, async (target, children) => {
    const each = Math.ceil(jobs / producers);
    const procs = await startTogether(
      (i) => new Child("producer-proc.ts", [JSON.stringify(target), "burst", String(each), String(keyed), `p${i}`]),
      producers,
    );
    children.push(...procs);
    const reports = (await Promise.all(procs.map((p) => p.next("done")))) as unknown as { callMs: number[] }[];

    const rows = await readRows(target);
    const total = each * producers;
    if (rows.length !== total) {
      throw new CorrectnessError([`expected ${total} jobs, found ${rows.length}`], NO_FACTS);
    }
    const createdAt = rows.map((r) => r.createdAt);
    return {
      jobsPerSecond: (total * 1000) / Math.max(1, max(createdAt) - min(createdAt)),
      callMs: distribution(reports.flatMap((r) => r.callMs)),
    };
  });
}

// --- S3 latency at a steady rate ----------------------------------------------

export interface LatencyResult {
  /**
   * claimedAt − runAt: how long a job waited for a worker after its row was
   * written. Starts at the write, not the intended send time, so unlike
   * endToEndMs it doesn't include producer delay.
   */
  claimWaitMs: Distribution;
  /** finishedAt − intended send time: the whole trip, including any producer delay. */
  endToEndMs: Distribution;
  /** How far behind schedule the producer's worst send was. */
  producerMaxLagMs: number;
  /**
   * The most jobs pending or running at once during the run. If this keeps
   * growing, the rate was above capacity, not the share of it the row claims.
   */
  maxUnfinished: number;
}

/** S3: workers running first, then one producer sends `perSecond` jobs/s for `durationMs`. */
export function latency(
  backend: Backend,
  layout: Layout,
  perSecond: number,
  durationMs: number,
  pollIntervalMs: number,
): Promise<LatencyResult> {
  return withTarget(backend, async (target, children) => {
    const workers = await startWorkers(target, layout, pollIntervalMs);
    children.push(...workers);
    const [producer] = await startTogether(
      () => new Child("producer-proc.ts", [JSON.stringify(target), "rate", String(perSecond), String(durationMs)]),
      1,
    );
    children.push(producer!);

    // Sample the backlog while the producer runs.
    const progress = await ProgressReader.open(target);
    let maxUnfinished = 0;
    let producing = true;
    const sampling = (async () => {
      while (producing) {
        maxUnfinished = Math.max(maxUnfinished, await progress.unfinished());
        await sleep(100);
      }
    })();
    let done: { sent: number; maxLagMs: number };
    try {
      done = (await producer!.next("done")) as unknown as { sent: number; maxLagMs: number };
    } finally {
      producing = false;
      await sampling;
      await progress.close();
    }

    await waitForDrained(target);
    const reports = await stopWorkers(workers);

    const rows = await readRows(target);
    checkCompleted(rows, done.sent, reports);
    return {
      claimWaitMs: distribution(rows.map((r) => r.claimedAt! - r.runAt)),
      endToEndMs: distribution(rows.map((r) => r.finishedAt! - (r.payload as { t: number }).t)),
      producerMaxLagMs: done.maxLagMs,
      maxUnfinished,
    };
  });
}
