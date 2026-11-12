// Child process used by claim-concurrency.test.ts. Opens its own connection to a
// shared database file, claims and completes jobs until the queue is empty, then
// prints the ids it handled as JSON on stdout.
import { SqliteStorage } from "../../src/index.js";

const [dbPath, workerId] = process.argv.slice(2);
if (!dbPath || !workerId) throw new Error("usage: claim-worker <dbPath> <workerId>");

const store = new SqliteStorage({ path: dbPath });
const handled: string[] = [];

for (;;) {
  const job = await store.claim("q", workerId);
  if (!job) break;
  handled.push(job.id);
  await store.complete(job.id, workerId);
}

await store.close();
console.log(JSON.stringify(handled));
