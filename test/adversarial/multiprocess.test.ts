// Exactly-once claiming under real multi-process contention, including the case
// the existing tests are least likely to cover: leases lapsing constantly, so
// processes race to *re-claim* stalled jobs, not just pending ones. Also checks
// stats() stays a consistent snapshot while other processes change job states.
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { afterEach, describe, expect, it } from "vitest";
import { BACKENDS, sleep, type Location, type Opened } from "./helpers/backends.js";

const CLAIMER = fileURLToPath(new URL("./helpers/claimer-process.ts", import.meta.url));

interface Claim {
  id: string;
  attempts: number;
  worker: string;
}

function runClaimer(args: Record<string, unknown>): Promise<{ claims: Claim[]; stderr: string }> {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, ["--import", "tsx", CLAIMER, JSON.stringify(args)], {
      stdio: ["ignore", "pipe", "pipe"],
    });
    let out = "";
    let err = "";
    child.stdout.on("data", (d) => (out += d));
    child.stderr.on("data", (d) => (err += d));
    child.on("error", reject);
    child.on("exit", (code) => {
      if (code !== 0) return reject(new Error(`claimer exited ${code}: ${err}`));
      const claims = out.split("\n").filter(Boolean).map((l) => JSON.parse(l) as Claim);
      resolve({ claims, stderr: err });
    });
  });
}

function runClaimers(n: number, location: Location, queue: string, mode: string, idleRounds: number) {
  const startAt = Date.now() + 1_500; // time for every process to boot first
  return Promise.all(
    Array.from({ length: n }, (_, i) => runClaimer({ location, queue, workerId: `p${i}`, startAt, mode, idleRounds })),
  );
}

describe.each(BACKENDS)("$name: multi-process", (backend) => {
  let opened: Opened | undefined;
  afterEach(async () => {
    await opened?.cleanup();
    opened = undefined;
  });

  it("S1/S4/S5: with leases lapsing constantly, every (job, attempt) is handed to exactly one process and every job ends dead after exactly maxAttempts claims", async () => {
    opened = await backend.open();
    const s = opened.storage;
    const JOBS = 60;
    const MAX = 4;
    const ids: string[] = [];
    for (let i = 0; i < JOBS; i++) ids.push((await s.enqueue({ queue: "q", payload: i, maxAttempts: MAX, leaseMs: 15 })).id);

    const results = await runClaimers(6, opened.location, "q", "hold", 40);
    const claims = results.flatMap((r) => r.claims);

    const seen = new Map<string, Claim>();
    const duplicates: string[] = [];
    for (const c of claims) {
      const key = `${c.id}#${c.attempts}`;
      const prior = seen.get(key);
      if (prior) duplicates.push(`${key} claimed by ${prior.worker} and ${c.worker}`);
      seen.set(key, c);
    }
    expect(duplicates).toEqual([]);

    // Let the last leases lapse, then one more claim expires the exhausted jobs.
    await sleep(50);
    expect(await s.claim("q", "sweeper")).toBeNull();
    for (const id of ids) {
      const job = await s.get(id);
      expect(job).toMatchObject({ status: "dead", attempts: MAX });
      const attemptsSeen = claims.filter((c) => c.id === id).map((c) => c.attempts).sort();
      expect(attemptsSeen).toEqual([1, 2, 3, 4]);
    }
  }, 60_000);

  it("ST1: every stats() snapshot taken while other processes claim/complete/fail/drop jobs accounts for every job exactly once", async () => {
    opened = await backend.open();
    const s = opened.storage;
    const JOBS = 200;
    for (let i = 0; i < JOBS; i++) await s.enqueue({ queue: "q", payload: i, leaseMs: 20, maxAttempts: 2 });

    let stop = false;
    const bad: string[] = [];
    let snapshots = 0;
    const poller = (async () => {
      while (!stop) {
        // SQLite's stats() settles synchronously; without a macrotask yield this
        // loop would starve the event loop and the child 'exit' events never fire.
        await new Promise((r) => setImmediate(r));
        const [row] = await s.stats("q");
        const all = await s.stats();
        snapshots++;
        if (!row) {
          bad.push("no row");
          continue;
        }
        const total = row.ready + row.scheduled + row.running + row.stalled + row.completed + row.dead;
        if (total !== JOBS) bad.push(`total ${total}: ${JSON.stringify(row)}`);
        if (Object.values(row).some((v) => typeof v === "number" && v < 0)) bad.push(`negative: ${JSON.stringify(row)}`);
        if (all.length !== 1) bad.push(`stats() listed ${all.length} queues`);
      }
    })();
    await runClaimers(6, opened.location, "q", "mixed", 30);
    stop = true;
    await poller;
    expect(snapshots).toBeGreaterThan(10);
    expect(bad).toEqual([]);
  }, 60_000);
});
