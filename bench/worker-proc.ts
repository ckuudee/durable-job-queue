// Child process: one Worker with a do-nothing handler, so what's measured is the
// queue's own overhead (claim, heartbeat, complete), not the handler.
//   argv: <target JSON> <concurrency> <pollIntervalMs> <workerId>
import { Worker } from "../src/index.js";
import { finish, fromParent, toParent } from "./lib/child.js";
import { openStorage, QUEUE, type Target } from "./lib/target.js";

const [targetJson, concurrencyArg, pollArg, workerId] = process.argv.slice(2);
if (!targetJson || !concurrencyArg || !pollArg || !workerId) {
  throw new Error("usage: worker-proc <target JSON> <concurrency> <pollIntervalMs> <workerId>");
}
const target = JSON.parse(targetJson) as Target;
const concurrency = Number(concurrencyArg);

// Listen for both up front, so neither message can arrive before its listener.
const go = fromParent("go");
const stop = fromParent("stop");

// One pooled connection per slot plus one spare for heartbeats, so the pool is
// never what the slots wait on.
const storage = await openStorage(target, concurrency + 1);
// PostgreSQL pools connect lazily. Open them all now, before "ready", so
// connection setup isn't counted in the timed run (larger layouts would pay
// more for it). Concurrent calls make the pool open one connection each.
await Promise.all(Array.from({ length: concurrency + 1 }, () => storage.get("warm-up")));

let abandoned = 0;
const errors: string[] = [];
// Every job the handler actually ran. The database can only say a job was
// claimed once; this is what shows the handler itself ran exactly once.
const handled: string[] = [];
const worker = new Worker(storage, {
  queue: QUEUE,
  workerId,
  concurrency,
  pollIntervalMs: Number(pollArg),
  handler: async (job) => {
    handled.push(job.id);
  },
  onError: (err) => errors.push(String(err)),
});
worker.on("job:abandoned", () => abandoned++);

await toParent({ type: "ready" });
await go;
worker.start();
await stop;
await worker.stop();
await storage.close();
await finish({ type: "done", abandoned, errors: errors.slice(0, 5), errorCount: errors.length, handled });
