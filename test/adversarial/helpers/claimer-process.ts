// Child process for multi-process contention tests. Usage:
//   node --import tsx claimer-process.ts '<json args>'
// Waits for a shared start time (so every process hits the database at once),
// then claims until the queue has been empty `idleRounds` times in a row,
// printing one JSON line per claim: {"id","attempts","worker"}.
import { openLocation, type Location } from "./backends.js";

interface Args {
  location: Location;
  queue: string;
  workerId: string;
  startAt: number;
  /** "hold": never complete (leases lapse); "complete": complete each job; "mixed": complete, fail or drop at random. */
  mode: "hold" | "complete" | "mixed";
  idleRounds: number;
}

const args = JSON.parse(process.argv[2] as string) as Args;
const storage = await openLocation(args.location);
while (Date.now() < args.startAt) await new Promise((r) => setTimeout(r, 1));

let idle = 0;
while (idle < args.idleRounds) {
  let job;
  try {
    job = await storage.claim(args.queue, args.workerId);
  } catch (err) {
    // SQLite under heavy contention can report SQLITE_BUSY; that's a retryable
    // failure, not a correctness violation, so note it and keep going.
    process.stderr.write(`claim error: ${(err as Error).message}\n`);
    continue;
  }
  if (!job) {
    idle++;
    await new Promise((r) => setTimeout(r, 5));
    continue;
  }
  idle = 0;
  process.stdout.write(`${JSON.stringify({ id: job.id, attempts: job.attempts, worker: args.workerId })}\n`);
  try {
    if (args.mode === "complete") await storage.complete(job.id, args.workerId);
    else if (args.mode === "mixed") {
      const r = Math.random();
      if (r < 0.4) await storage.complete(job.id, args.workerId);
      else if (r < 0.8) await storage.fail(job.id, args.workerId, "mixed failure");
      // else: drop it, as if this process crashed holding the job
    }
  } catch (err) {
    process.stderr.write(`result error: ${(err as Error).message}\n`);
  }
}
await storage.close();
