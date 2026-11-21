// Load-test runner. Method and scenarios: docs/design/load-tests.md.
//
//   npm run bench -- --backend sqlite                 # every scenario, full size
//   npm run bench -- --backend postgres --scenario drain
//   npm run bench -- --backend all --smoke            # tiny, correctness only (CI)
//
// PostgreSQL needs JOBQ_PG_URL. Each configuration runs `--warmup` discarded
// runs, then `--repeats` measured runs, each on a fresh database, and reports
// the median and range.
//
// Failures: a run that breaks a guarantee (a job lost, run twice, abandoned)
// never becomes a number. In the backlog sweep (S5) it is recorded as "fails at
// this size" and the sweep moves on to a smaller size; anywhere else it stops
// the runner. Harness errors (a crashed child, a stall) always stop it. Exit
// code 1 either way, after the results file is written.
import { execFileSync } from "node:child_process";
import { mkdirSync, writeFileSync } from "node:fs";
import { cpus, platform, release, totalmem } from "node:os";
import { join } from "node:path";
import { parseArgs } from "node:util";
import {
  abortAll,
  CorrectnessError,
  drain,
  enqueue,
  latency,
  type CurvePoint,
  type DrainResult,
  type Facts,
  type Layout,
} from "./scenarios.js";
import { median, spread, type Distribution, type Spread } from "./lib/stats.js";
import { databaseSettings, databaseVersions, type Backend } from "./lib/target.js";

const SCENARIOS = ["drain", "enqueue", "latency", "backlog", "curve"] as const;
type Scenario = (typeof SCENARIOS)[number];

const { values: opts } = parseArgs({
  options: {
    backend: { type: "string", default: "sqlite" },
    scenario: { type: "string", default: "all" },
    repeats: { type: "string" },
    warmup: { type: "string" },
    smoke: { type: "boolean", default: false },
    out: { type: "string" },
  },
});

const backends: Backend[] =
  opts.backend === "all" ? ["sqlite", "postgres"] : [parseChoice(opts.backend!, ["sqlite", "postgres"], "--backend")];
const scenarios: Scenario[] =
  opts.scenario === "all" ? [...SCENARIOS] : [parseChoice(opts.scenario!, SCENARIOS, "--scenario")];

// Full size follows the design doc. --smoke shrinks everything so CI can check
// that every code path runs and the guarantees hold, in about a minute.
const smoke = opts.smoke!;
const config = {
  repeats: Number(opts.repeats ?? (smoke ? 1 : 3)),
  warmup: Number(opts.warmup ?? (smoke ? 0 : 1)),
  // S1: each layout drains this many jobs; the headline is measured while the
  // pending backlog falls from 2 × lowBacklog to lowBacklog.
  drainJobs: smoke ? 300 : 3_000,
  lowBacklog: smoke ? 100 : 1_000,
  drainLayouts: smoke
    ? [layout(1, 1), layout(2, 2)]
    : [1, 2, 4, 8].flatMap((p) => [1, 4, 16].map((c) => layout(p, c))),
  enqueueJobs: smoke ? 300 : 20_000,
  producers: smoke ? [1, 2] : [1, 4],
  latencyLayout: smoke ? layout(2, 2) : layout(4, 4),
  latencyFractions: [0.25, 0.5, 0.75],
  latencyDurationMs: smoke ? 2_000 : 60_000,
  pollIntervals: [1000, 10],
  backlogSizes: smoke ? [0, 1_000] : [0, 100_000, 1_000_000],
  backlogLayout: smoke ? layout(2, 2) : layout(4, 4),
  // S5: one large drain gives the curve; sizes step down until one passes.
  curveLayout: smoke ? layout(2, 2) : layout(4, 4),
  curveSizes: smoke ? [600] : [60_000, 20_000, 5_000],
};

function layout(processes: number, concurrency: number): Layout {
  return { processes, concurrency };
}

function parseChoice<T extends string>(value: string, choices: readonly T[], flag: string): T {
  if (!(choices as readonly string[]).includes(value)) {
    throw new Error(`${flag} must be one of ${choices.join(", ")} (or all), got "${value}"`);
  }
  return value as T;
}

const label = (l: Layout) => `${l.processes}×${l.concurrency}`;
const fmt = (n: number) => (n >= 100 ? Math.round(n).toLocaleString("en-US") : n.toFixed(1));
/** Timestamps are whole milliseconds, so anything at or under 1 ms is reported as such, not as 0.0. */
const fmtMs = (n: number) => (n <= 1 ? "≤1" : fmt(n));
const fmtSpread = (s: Spread) => `${fmt(s.median)} (${fmt(s.min)}–${fmt(s.max)})`;
const count = (n: number) => n.toLocaleString("en-US");

/** Median p50 / p95 / p99 across runs, plus the worst max of any run (so one bad run can't hide). */
interface LatencySummary {
  p50: number;
  p95: number;
  p99: Spread;
  worstMax: number;
}
function summarize(runs: Distribution[]): LatencySummary {
  return {
    p50: median(runs.map((d) => d.p50)),
    p95: median(runs.map((d) => d.p95)),
    p99: spread(runs.map((d) => d.p99)),
    worstMax: Math.max(...runs.map((d) => d.max)),
  };
}
const fmtLatency = (s: LatencySummary) =>
  `${fmtMs(s.p50)} / ${fmtMs(s.p95)} / ${fmtMs(s.p99.median)} (${fmtMs(s.p99.min)}–${fmtMs(s.p99.max)}) / ${fmtMs(s.worstMax)}`;

/** A number that isn't finite and positive means the runner is broken, not the queue. */
function assertPositive(what: string, n: number | null): number {
  if (n === null || !Number.isFinite(n) || n <= 0) {
    throw new Error(`harness error: ${what} came out as ${n}, which can't be a real measurement`);
  }
  return n;
}
function assertLatency(what: string, d: Distribution): void {
  for (const [k, v] of Object.entries(d)) {
    if (!Number.isFinite(v) || v < 0) throw new Error(`harness error: ${what} ${k} came out as ${v}`);
  }
}

function progress(text: string): void {
  if (process.stderr.isTTY) process.stderr.write(`\r${text.padEnd(70)}\r`);
}

/** Stop the runner on a failure outside the backlog sweep (decision 2 in the review of #42). */
function fail(name: string, err: unknown): never {
  progress("");
  console.error(`\n${name}: ${err instanceof CorrectnessError ? err.message : err instanceof Error ? err.stack : err}`);
  process.exit(1);
}

/** Run `once` warmup + repeats times; return only the measured runs. Any failure stops the runner. */
async function repeat<T>(name: string, once: () => Promise<T>): Promise<T[]> {
  const results: T[] = [];
  const total = config.warmup + config.repeats;
  for (let i = 0; i < total; i++) {
    const warm = i < config.warmup;
    progress(`  ${name}: run ${i + 1}/${total}${warm ? " (warm-up)" : ""}…`);
    try {
      const result = await once();
      if (!warm) results.push(result);
    } catch (err) {
      fail(`${name}, run ${i + 1}`, err);
    }
  }
  progress("");
  return results;
}

// Ctrl-C or a cancelled CI job: kill the workers and delete the run's database.
for (const [signal, code] of [
  ["SIGINT", 130],
  ["SIGTERM", 143],
] as const) {
  process.on(signal, () => {
    console.error(`\n${signal}: stopping workers and deleting the run's database…`);
    void abortAll().finally(() => process.exit(code));
  });
}

const results: Record<string, unknown> = {};
const settings: Record<string, Record<string, string>> = {};
let guaranteeBroken = false;

for (const backend of backends) {
  const out: Record<string, unknown> = {};
  results[backend] = out;
  const backendSettings = await databaseSettings(backend);
  settings[backend] = backendSettings;
  console.log(`\n## ${backend}\n`);

  // PostgreSQL refuses connections past max_connections, and the default (100)
  // is kept on purpose. Each worker process opens concurrency + 1; the harness
  // needs a couple more.
  const connectionBudget =
    backend === "postgres"
      ? Number(backendSettings.max_connections) - Number(backendSettings.superuser_reserved_connections) - 4
      : Infinity;
  const connectionsFor = (l: Layout) => l.processes * (l.concurrency + 1);
  const fits = (l: Layout) => connectionsFor(l) <= connectionBudget;

  // S1. Run first: S3's rates are shares of the S1 result for its layout.
  let peakForLatency: number | undefined;
  if (scenarios.includes("drain") || scenarios.includes("latency")) {
    const layouts = scenarios.includes("drain") ? config.drainLayouts : [config.latencyLayout];
    const rows = [];
    console.log(
      `S1 drain: ${count(config.drainJobs)} jobs per run; jobs/s while ${count(2 * config.lowBacklog)}→` +
        `${count(config.lowBacklog)} jobs were pending, median (min–max)\n`,
    );
    console.log("| processes × slots | jobs/s at low backlog | whole drain, jobs/s |\n|---|---|---|");
    for (const l of layouts) {
      if (!fits(l)) {
        const reason = `not run: needs ${connectionsFor(l)} connections, server allows ${backendSettings.max_connections}`;
        rows.push({ layout: l, skipped: reason });
        console.log(`| ${label(l)} | ${reason} | |`);
        continue;
      }
      const runs = await repeat(`drain ${label(l)}`, () =>
        drain(backend, l, config.drainJobs, { lowBacklog: config.lowBacklog }),
      );
      const low = spread(runs.map((r) => assertPositive("low-backlog jobs/s", r.lowBacklogJobsPerSecond)));
      const overall = spread(runs.map((r) => assertPositive("whole-drain jobs/s", r.overallJobsPerSecond)));
      rows.push({
        layout: l,
        lowBacklogJobsPerSecond: low,
        overallJobsPerSecond: overall,
        runs: runs.map((r) => ({ lowBacklog: r.lowBacklogJobsPerSecond, overall: r.overallJobsPerSecond })),
      });
      console.log(`| ${label(l)} | ${fmtSpread(low)} | ${fmtSpread(overall)} |`);
      if (label(l) === label(config.latencyLayout)) peakForLatency = low.median;
    }
    out.drain = rows;
  }

  if (scenarios.includes("enqueue")) {
    console.log(
      `\nS2 enqueue: ${count(config.enqueueJobs)} jobs; jobs/s median (min–max); ` +
        `enqueue() ms p50 / p95 / p99 (range) / worst max\n`,
    );
    console.log("| producers | idempotency key | jobs/s | enqueue() ms |\n|---|---|---|---|");
    const rows = [];
    for (const producers of config.producers) {
      for (const keyed of [false, true]) {
        const runs = await repeat(`enqueue ${producers}p${keyed ? " keyed" : ""}`, () =>
          enqueue(backend, producers, config.enqueueJobs, keyed),
        );
        runs.forEach((r) => assertLatency("enqueue() ms", r.callMs));
        const jobsPerSecond = spread(runs.map((r) => assertPositive("enqueue jobs/s", r.jobsPerSecond)));
        const callMs = summarize(runs.map((r) => r.callMs));
        rows.push({ producers, keyed, jobsPerSecond, callMs, runs });
        console.log(`| ${producers} | ${keyed ? "yes" : "no"} | ${fmtSpread(jobsPerSecond)} | ${fmtLatency(callMs)} |`);
      }
    }
    out.enqueue = rows;
  }

  if (scenarios.includes("latency")) {
    const l = config.latencyLayout;
    const peak = assertPositive("S1 low-backlog rate for the latency layout", peakForLatency ?? null);
    console.log(
      `\nS3 latency: ${label(l)} workers, ${config.latencyDurationMs / 1000} s per run, rates as a share of ` +
        `${fmt(peak)} jobs/s (S1 at low backlog); ms p50 / p95 / p99 (range) / worst max\n`,
    );
    console.log(
      "| load | jobs/s | poll ms | claim wait | end to end | most unfinished | producer max lag ms |\n" +
        "|---|---|---|---|---|---|---|",
    );
    const rows = [];
    for (const fraction of config.latencyFractions) {
      const perSecond = Math.max(1, Math.round(peak * fraction));
      for (const poll of config.pollIntervals) {
        const runs = await repeat(`latency ${perSecond}/s poll ${poll}`, () =>
          latency(backend, l, perSecond, config.latencyDurationMs, poll),
        );
        runs.forEach((r) => {
          assertLatency("claim wait", r.claimWaitMs);
          assertLatency("end to end", r.endToEndMs);
        });
        const claimWaitMs = summarize(runs.map((r) => r.claimWaitMs));
        const endToEndMs = summarize(runs.map((r) => r.endToEndMs));
        const mostUnfinished = Math.max(...runs.map((r) => r.maxUnfinished));
        const lag = Math.max(...runs.map((r) => r.producerMaxLagMs));
        rows.push({ fraction, perSecond, pollIntervalMs: poll, claimWaitMs, endToEndMs, mostUnfinished, runs });
        console.log(
          `| ${fraction * 100}% | ${perSecond} | ${poll} | ${fmtLatency(claimWaitMs)} | ${fmtLatency(endToEndMs)} | ` +
            `${count(mostUnfinished)} | ${fmt(lag)} |`,
        );
      }
    }
    out.latency = rows;
  }

  if (scenarios.includes("backlog")) {
    const l = config.backlogLayout;
    console.log(
      `\nS4 completed rows: drain ${count(config.drainJobs)} jobs with ${label(l)} workers; ` +
        `jobs/s at low backlog, median (min–max)\n`,
    );
    console.log("| completed rows already in table | jobs/s |\n|---|---|");
    const rows = [];
    for (const seeded of config.backlogSizes) {
      const runs = await repeat(`completed rows ${count(seeded)}`, () =>
        drain(backend, l, config.drainJobs, { seeded, lowBacklog: config.lowBacklog }),
      );
      const jobsPerSecond = spread(runs.map((r) => assertPositive("S4 jobs/s", r.lowBacklogJobsPerSecond)));
      rows.push({ seeded, jobsPerSecond, plan: runs[0]?.plan, runs: runs.map((r) => r.lowBacklogJobsPerSecond) });
      console.log(`| ${count(seeded)} | ${fmtSpread(jobsPerSecond)} |`);
    }
    const biggest = rows[rows.length - 1];
    if (biggest?.plan) console.log(`\nClaim query plan with ${count(biggest.seeded)} completed rows:\n${biggest.plan}`);
    out.backlog = rows;
  }

  if (scenarios.includes("curve")) {
    const l = config.curveLayout;
    console.log(
      `\nS5 throughput vs pending backlog: one drain per run with ${label(l)} workers, read from completion times. ` +
        `Sizes step down until every run passes.\n`,
    );
    const sizes = [];
    for (const size of config.curveSizes) {
      // ~50 points per curve, at most 1,000 completions each.
      const curveWindow = Math.min(1000, Math.max(50, Math.round(size / 50)));
      const total = config.warmup + config.repeats;
      const passed: DrainResult[] = [];
      const failures: Facts[] = [];
      for (let i = 0; i < total; i++) {
        progress(`  curve ${count(size)}: run ${i + 1}/${total}${i < config.warmup ? " (warm-up)" : ""}…`);
        try {
          const r = await drain(backend, l, size, { curveWindow, lowBacklog: config.lowBacklog });
          if (i >= config.warmup) passed.push(r);
        } catch (err) {
          // Only a broken guarantee is a finding; anything else is the harness's fault.
          if (!(err instanceof CorrectnessError)) fail(`curve ${count(size)}, run ${i + 1}`, err);
          failures.push(err.facts);
        }
      }
      progress("");
      if (failures.length > 0) {
        // No curve from a size where any run failed: a job that ran twice carries
        // its second attempt's timestamps, and keeping only the passing runs
        // would hide how often it fails (survivorship bias).
        guaranteeBroken = true;
        const f = failures[0]!;
        sizes.push({ size, failedRuns: failures.length, of: total, facts: failures });
        console.log(
          `- ${count(size)} jobs: **failed ${failures.length} of ${total} runs**. First failure: ` +
            `${f.claimedMoreThanOnce} claimed more than once, ${f.handlerNotOnce} handler not run exactly once, ` +
            `${f.abandoned} abandoned, ${f.workerErrors} worker errors` +
            (f.firstError ? ` (e.g. ${f.firstError})` : ""),
        );
        continue;
      }
      const points: CurvePoint[] = passed[0]!.curve.map((p, i) => ({
        pending: p.pending,
        jobsPerSecond: median(passed.map((r) => assertPositive("curve jobs/s", r.curve[i]?.jobsPerSecond ?? null))),
      }));
      sizes.push({ size, failedRuns: 0, of: total, curve: points, runs: passed.map((r) => r.curve) });
      console.log(`- ${count(size)} jobs: passed ${total} of ${total} runs. Median jobs/s by pending backlog:\n`);
      console.log("| pending | jobs/s |\n|---|---|");
      const step = Math.max(1, Math.floor(points.length / 12)); // print about a dozen rows; the JSON has them all
      points
        .filter((_, i) => i % step === 0)
        .forEach((p) => console.log(`| ${count(p.pending)} | ${fmt(p.jobsPerSecond)} |`));
      break; // the smaller sizes' backlogs are already covered by this curve
    }
    out.curve = { layout: l, sizes };
  }
}

// Where the numbers came from, so a reader can judge (or reproduce) them.
const git = (args: string[]) => execFileSync("git", args, { encoding: "utf8" }).trim();
const report = {
  date: new Date().toISOString(),
  smoke,
  config,
  machine: {
    cpu: cpus()[0]?.model,
    cores: cpus().length,
    memoryGb: Math.round(totalmem() / 2 ** 30),
    os: `${platform()} ${release()}`,
    arch: process.arch,
    node: process.version,
  },
  databases: await databaseVersions(backends),
  settings,
  commit: git(["rev-parse", "HEAD"]),
  uncommittedChanges: git(["status", "--porcelain"]) !== "",
  guaranteeBroken,
  results,
};

if (opts.out || !smoke) {
  const stamp = report.date.replace(/[:.]/g, "-").slice(0, 19); // two runs a day mustn't overwrite each other
  const file = opts.out ?? join(import.meta.dirname, "results", `${backends.join("+")}-${stamp}.json`);
  mkdirSync(join(file, ".."), { recursive: true });
  writeFileSync(file, JSON.stringify(report, null, 2) + "\n");
  console.log(`\nWrote ${file}`);
}

if (guaranteeBroken) {
  console.error("\nA guarantee broke in the backlog sweep (see above). Exiting with code 1.");
  process.exit(1);
}
