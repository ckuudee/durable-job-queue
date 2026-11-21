// One function per scenario in docs/design/load-tests.md. Each call is one run
// on a fresh database: set up, run the child processes, read the job rows back,
// check correctness, compute the numbers, and delete the database.
import { setTimeout as sleep } from "node:timers/promises";
import { Child, startTogether } from "./lib/child.js";
import { distribution, type Distribution } from "./lib/stats.js";
import {
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

/** Thrown when a run breaks a guarantee; the runner reports it instead of a number. */
export class CorrectnessError extends Error {
  constructor(problems: string[]) {
    super(`correctness check failed:\n  - ${problems.join("\n  - ")}`);
    this.name = "CorrectnessError";
  }
}

interface WorkerReport {
  abandoned: number;
  errorCount: number;
  errors: string[];
}

/**
 * The correctness check every run passes before its numbers count: every job
 * enqueued completed exactly once (attempts = 1, since nothing crashes and
 * leases are 30 s), and no worker gave a job up or hit an error.
 */
function checkCompleted(rows: Row[], expected: number, workers: WorkerReport[]): void {
  const problems: string[] = [];
  if (rows.length !== expected) problems.push(`expected ${expected} jobs, found ${rows.length}`);
  const notDone = rows.filter((r) => r.status !== "completed").length;
  if (notDone > 0) problems.push(`${notDone} jobs not completed`);
  const repeated = rows.filter((r) => r.attempts !== 1).length;
  if (repeated > 0) problems.push(`${repeated} jobs with attempts != 1 (ran more than once)`);
  const abandoned = workers.reduce((n, w) => n + w.abandoned, 0);
  if (abandoned > 0) problems.push(`${abandoned} jobs abandoned by workers`);
  const errors = workers.flatMap((w) => w.errors);
  const errorCount = workers.reduce((n, w) => n + w.errorCount, 0);
  if (errorCount > 0) problems.push(`${errorCount} worker errors, e.g. ${errors[0]}`);
  if (problems.length > 0) throw new CorrectnessError(problems);
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

/** Create a target, run `fn`, and always delete the target and stop stray children. */
async function withTarget<T>(backend: Backend, fn: (target: Target, children: Child[]) => Promise<T>): Promise<T> {
  const target = await createTarget(backend);
  const children: Child[] = [];
  try {
    return await fn(target, children);
  } finally {
    for (const c of children) c.kill();
    await destroyTarget(target);
  }
}

// --- S1 drain and S4 large tables --------------------------------------------

export interface DrainResult {
  jobsPerSecond: number;
  /** Only for S4: the claim query's plan with the seeded rows present. */
  plan?: string;
}

/**
 * S1 (and S4 with `seeded` > 0): preload `jobs` ready jobs, start every worker
 * at once, and time the drain from the first claim to the last completion.
 */
export function drain(backend: Backend, layout: Layout, jobs: number, seeded = 0): Promise<DrainResult> {
  return withTarget(backend, async (target, children) => {
    await seedCompleted(target, seeded);
    await preload(target, jobs);
    const plan = seeded > 0 ? await explainClaim(target) : undefined;

    const workers = await startWorkers(target, layout, 1000);
    children.push(...workers);
    await waitForDrained(target);
    const reports = await stopWorkers(workers);

    const rows = await readRows(target);
    checkCompleted(rows, jobs, reports);
    const firstClaim = min(rows.map((r) => r.claimedAt!));
    const lastFinish = max(rows.map((r) => r.finishedAt!));
    return { jobsPerSecond: (jobs * 1000) / Math.max(1, lastFinish - firstClaim), plan };
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
    if (rows.length !== total) throw new CorrectnessError([`expected ${total} jobs, found ${rows.length}`]);
    const first = min(rows.map((r) => r.createdAt));
    const last = max(rows.map((r) => r.createdAt));
    return {
      jobsPerSecond: (total * 1000) / Math.max(1, last - first),
      callMs: distribution(reports.flatMap((r) => r.callMs)),
    };
  });
}

// --- S3 latency at a steady rate ----------------------------------------------

export interface LatencyResult {
  /** claimedAt − runAt: how long a ready job waited for a worker. */
  claimWaitMs: Distribution;
  /** finishedAt − intended send time: the whole trip, including any producer delay. */
  endToEndMs: Distribution;
  /** How far behind schedule the producer's worst send was. */
  producerMaxLagMs: number;
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
    const { sent, maxLagMs } = (await producer!.next("done")) as unknown as { sent: number; maxLagMs: number };
    await waitForDrained(target);
    const reports = await stopWorkers(workers);

    const rows = await readRows(target);
    checkCompleted(rows, sent, reports);
    return {
      claimWaitMs: distribution(rows.map((r) => r.claimedAt! - r.runAt)),
      endToEndMs: distribution(rows.map((r) => r.finishedAt! - (r.payload as { t: number }).t)),
      producerMaxLagMs: maxLagMs,
    };
  });
}
