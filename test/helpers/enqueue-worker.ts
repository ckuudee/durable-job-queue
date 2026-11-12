// Child process for the idempotent-enqueue race. Enqueues the same set of keyed
// jobs as every other worker, starting at a different position so the processes
// collide on different keys at different times. Prints a {key: jobId} map as JSON.
import { SqliteStorage } from "../../src/index.js";

const [dbPath, keyCountArg, offsetArg] = process.argv.slice(2);
if (!dbPath || !keyCountArg || !offsetArg) {
  throw new Error("usage: enqueue-worker <dbPath> <keyCount> <offset>");
}
const keyCount = Number(keyCountArg);
const offset = Number(offsetArg);

const store = new SqliteStorage({ path: dbPath });
const ids: Record<string, string> = {};

for (let i = 0; i < keyCount; i++) {
  const n = (i + offset) % keyCount;
  const job = await store.enqueue({ queue: "q", payload: { n }, idempotencyKey: `key-${n}` });
  ids[`key-${n}`] = job.id;
}

await store.close();
console.log(JSON.stringify(ids));
