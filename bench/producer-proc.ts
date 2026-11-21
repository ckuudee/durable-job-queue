// Child process that enqueues jobs. Two modes:
//
//   burst <count> <keyed> <producerId>
//     Enqueue `count` jobs back to back, awaiting each call (like a request
//     handler that enqueues and then responds). Reports each call's duration.
//
//   rate <jobsPerSecond> <durationMs>
//     Open-loop load: job i is due at start + i / rate, and is sent at that time
//     whether or not earlier calls have returned. Each payload carries its
//     intended send time `t`, so latency is measured from when the job *should*
//     have been enqueued. If the producer falls behind (say, blocked waiting on
//     SQLite's write lock), that delay counts against the queue instead of being
//     silently skipped. Skipping it is the "coordinated omission" error.
//
//   argv: <target JSON> <mode> ...mode args
import { performance } from "node:perf_hooks";
import { setTimeout as sleep } from "node:timers/promises";
import { finish, fromParent, toParent } from "./lib/child.js";
import { openStorage, QUEUE, type Target } from "./lib/target.js";

const [targetJson, mode, ...rest] = process.argv.slice(2);
if (!targetJson || (mode !== "burst" && mode !== "rate")) {
  throw new Error("usage: producer-proc <target JSON> burst <count> <keyed> <producerId> | rate <perSecond> <durationMs>");
}
const target = JSON.parse(targetJson) as Target;
const go = fromParent("go");
const storage = await openStorage(target, 16);
await toParent({ type: "ready" });
await go;

if (mode === "burst") {
  const count = Number(rest[0]);
  const keyed = rest[1] === "true";
  const producerId = rest[2] ?? "p";
  const callMs: number[] = [];
  for (let i = 0; i < count; i++) {
    const started = performance.now();
    await storage.enqueue({
      queue: QUEUE,
      payload: { n: i },
      ...(keyed ? { idempotencyKey: `${producerId}-${i}` } : {}),
    });
    callMs.push(performance.now() - started);
  }
  await storage.close();
  await finish({ type: "done", callMs });
} else {
  const perSecond = Number(rest[0]);
  const durationMs = Number(rest[1]);
  const total = Math.floor((perSecond * durationMs) / 1000);
  const start = Date.now() + 20;
  const inFlight: Promise<unknown>[] = [];
  let maxLagMs = 0;
  for (let i = 0; i < total; i++) {
    const due = start + (i * 1000) / perSecond;
    const wait = due - Date.now();
    if (wait >= 1) await sleep(wait);
    maxLagMs = Math.max(maxLagMs, Date.now() - due);
    inFlight.push(storage.enqueue({ queue: QUEUE, payload: { t: due } }));
  }
  await Promise.all(inFlight);
  await storage.close();
  await finish({ type: "done", sent: total, maxLagMs });
}
