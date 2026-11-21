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
let abandoned = 0;
const errors: string[] = [];
const worker = new Worker(storage, {
  queue: QUEUE,
  workerId,
  concurrency,
  pollIntervalMs: Number(pollArg),
  handler: async () => {},
  onError: (err) => errors.push(String(err)),
});
worker.on("job:abandoned", () => abandoned++);

await toParent({ type: "ready" });
await go;
worker.start();
await stop;
await worker.stop();
await storage.close();
await finish({ type: "done", abandoned, errors: errors.slice(0, 5), errorCount: errors.length });
