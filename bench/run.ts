// Load-test runner. Method and scenarios: docs/design/load-tests.md.
//
//   npm run bench -- --backend sqlite                 # every scenario, full size
//   npm run bench -- --backend postgres --scenario drain
//   npm run bench -- --backend all --smoke            # tiny, correctness only (CI)
//
// PostgreSQL needs JOBQ_PG_URL. Each configuration runs `--warmup` discarded
// runs, then `--repeats` measured runs, each on a fresh database, and reports
// the median and range. A run that fails the correctness check stops
// everything with exit code 1: a wrong result is never reported as a number.
import { execFileSync } from "node:child_process";
import { mkdirSync, writeFileSync } from "node:fs";
import { cpus, platform, release, totalmem } from "node:os";
import { join } from "node:path";
import { parseArgs } from "node:util";
import { CorrectnessError, drain, enqueue, latency, type Layout } from "./scenarios.js";
import { medianDistribution, spread, type Distribution, type Spread } from "./lib/stats.js";
import { databaseVersions, type Backend } from "./lib/target.js";

const SCENARIOS = ["drain", "enqueue", "latency", "backlog"] as const;
type Scenario = (typeof SCENARIOS)[number];

const { values: opts } = parseArgs({
  options: {
    backend: { type: "string", default: "sqlite" },
    scenario: { type: "string", default: "all" },
    jobs: { type: "string" },
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

// Full size follows the design doc; --smoke shrinks everything so CI can check
// that the runner works and the guarantees hold, in well under a minute.
const smoke = opts.smoke!;
const config = {
  jobs: Number(opts.jobs ?? (smoke ? 300 : 20_000)),
  repeats: Number(opts.repeats ?? (smoke ? 1 : 3)),
  warmup: Number(opts.warmup ?? (smoke ? 0 : 1)),
  drainLayouts: smoke
    ? [layout(1, 1), layout(2, 4)]
    : [1, 2, 4, 8].flatMap((p) => [1, 4, 16].map((c) => layout(p, c))),
  producers: smoke ? [1, 2] : [1, 4],
  latencyLayout: smoke ? layout(2, 2) : layout(4, 4),
  latencyFractions: [0.25, 0.5, 0.75],
  latencyDurationMs: smoke ? 2_000 : 60_000,
  pollIntervals: [1000, 10],
  backlogSizes: smoke ? [0, 1_000] : [0, 100_000, 1_000_000],
  backlogLayout: smoke ? layout(2, 2) : layout(4, 4),
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
const fmtSpread = (s: Spread) => `${fmt(s.median)} (${fmt(s.min)}–${fmt(s.max)})`;
const fmtDist = (d: Distribution) => `${fmt(d.p50)} / ${fmt(d.p95)} / ${fmt(d.p99)} / ${fmt(d.max)}`;

/** Run `once` warmup + repeats times; return only the measured runs. */
async function repeat<T>(name: string, once: () => Promise<T>): Promise<T[]> {
  const results: T[] = [];
  const total = config.warmup + config.repeats;
  for (let i = 0; i < total; i++) {
    const warm = i < config.warmup;
    progress(`  ${name}: run ${i + 1}/${total}${warm ? " (warm-up)" : ""}…`);
    let result: T;
    try {
      result = await once();
    } catch (err) {
      // A broken guarantee is the most important thing a load test can find:
      // say so plainly and fail, rather than print numbers for a wrong run.
      progress("");
      console.error(`\n${name}, run ${i + 1}: ${err instanceof CorrectnessError ? err.message : err}`);
      process.exit(1);
    }
    if (!warm) results.push(result);
  }
  progress("");
  return results;
}

/** A one-line status that overwrites itself; only in a terminal, so logs stay clean. */
function progress(text: string): void {
  if (process.stderr.isTTY) process.stderr.write(`\r${text.padEnd(70)}\r`);
}

const results: Record<string, unknown> = {};

for (const backend of backends) {
  const out: Record<string, unknown> = {};
  results[backend] = out;
  console.log(`\n## ${backend}\n`);

  // S1. Run first: S3's rates are fractions of the S1 result for its layout.
  let peakForLatency: number | undefined;
  if (scenarios.includes("drain") || scenarios.includes("latency")) {
    const layouts = scenarios.includes("drain") ? config.drainLayouts : [config.latencyLayout];
    const rows = [];
    console.log(`S1 drain: ${config.jobs.toLocaleString("en-US")} jobs, jobs/s median (min–max)\n`);
    console.log("| processes × slots | jobs/s |\n|---|---|");
    for (const l of layouts) {
      const runs = await repeat(`drain ${label(l)}`, () => drain(backend, l, config.jobs));
      const jobsPerSecond = spread(runs.map((r) => r.jobsPerSecond));
      rows.push({ layout: l, jobsPerSecond, runs: runs.map((r) => r.jobsPerSecond) });
      console.log(`| ${label(l)} | ${fmtSpread(jobsPerSecond)} |`);
      if (label(l) === label(config.latencyLayout)) peakForLatency = jobsPerSecond.median;
    }
    out.drain = rows;
  }

  if (scenarios.includes("enqueue")) {
    console.log(`\nS2 enqueue: ${config.jobs.toLocaleString("en-US")} jobs, jobs/s median (min–max); call ms p50 / p95 / p99 / max\n`);
    console.log("| producers | idempotency key | jobs/s | enqueue() ms |\n|---|---|---|---|");
    const rows = [];
    for (const producers of config.producers) {
      for (const keyed of [false, true]) {
        const runs = await repeat(`enqueue ${producers}p${keyed ? " keyed" : ""}`, () =>
          enqueue(backend, producers, config.jobs, keyed),
        );
        const jobsPerSecond = spread(runs.map((r) => r.jobsPerSecond));
        const callMs = medianDistribution(runs.map((r) => r.callMs));
        rows.push({ producers, keyed, jobsPerSecond, callMs });
        console.log(`| ${producers} | ${keyed ? "yes" : "no"} | ${fmtSpread(jobsPerSecond)} | ${fmtDist(callMs)} |`);
      }
    }
    out.enqueue = rows;
  }

  if (scenarios.includes("latency")) {
    const l = config.latencyLayout;
    const peak = smoke ? 100 : peakForLatency!;
    console.log(
      `\nS3 latency: ${label(l)} workers, ${config.latencyDurationMs / 1000} s per run, ` +
        `rates as a share of ${fmt(peak)} jobs/s; ms p50 / p95 / p99 / max\n`,
    );
    console.log("| load | jobs/s | poll ms | claim wait | end to end | producer max lag |\n|---|---|---|---|---|---|");
    const rows = [];
    for (const fraction of config.latencyFractions) {
      const perSecond = Math.max(1, Math.round(peak * fraction));
      for (const poll of config.pollIntervals) {
        const runs = await repeat(`latency ${perSecond}/s poll ${poll}`, () =>
          latency(backend, l, perSecond, config.latencyDurationMs, poll),
        );
        const claimWaitMs = medianDistribution(runs.map((r) => r.claimWaitMs));
        const endToEndMs = medianDistribution(runs.map((r) => r.endToEndMs));
        const lag = spread(runs.map((r) => r.producerMaxLagMs));
        rows.push({ fraction, perSecond, pollIntervalMs: poll, claimWaitMs, endToEndMs, producerMaxLagMs: lag });
        console.log(
          `| ${fraction * 100}% | ${perSecond} | ${poll} | ${fmtDist(claimWaitMs)} | ${fmtDist(endToEndMs)} | ${fmt(lag.median)} |`,
        );
      }
    }
    out.latency = rows;
  }

  if (scenarios.includes("backlog")) {
    const l = config.backlogLayout;
    console.log(`\nS4 large tables: drain ${config.jobs.toLocaleString("en-US")} jobs with ${label(l)} workers\n`);
    console.log("| completed rows already in table | jobs/s |\n|---|---|");
    const rows = [];
    for (const seeded of config.backlogSizes) {
      const runs = await repeat(`backlog ${seeded}`, () => drain(backend, l, config.jobs, seeded));
      const jobsPerSecond = spread(runs.map((r) => r.jobsPerSecond));
      rows.push({ seeded, jobsPerSecond, plan: runs[0]?.plan });
      console.log(`| ${seeded.toLocaleString("en-US")} | ${fmtSpread(jobsPerSecond)} |`);
    }
    const biggest = rows[rows.length - 1];
    if (biggest?.plan) console.log(`\nClaim query plan with ${biggest.seeded.toLocaleString("en-US")} rows:\n${biggest.plan}`);
    out.backlog = rows;
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
  commit: git(["rev-parse", "HEAD"]),
  uncommittedChanges: git(["status", "--porcelain"]) !== "",
  results,
};

if (opts.out || !smoke) {
  const file =
    opts.out ?? join(import.meta.dirname, "results", `${backends.join("+")}-${report.date.slice(0, 10)}.json`);
  mkdirSync(join(file, ".."), { recursive: true });
  writeFileSync(file, JSON.stringify(report, null, 2) + "\n");
  console.log(`\nWrote ${file}`);
}
