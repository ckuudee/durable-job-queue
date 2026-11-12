// Child process used by the multi-process tests. Opens its own connection to a
// shared database file, claims jobs until the queue is empty and either
// completes or fails each one, then prints the ids it handled as JSON on stdout.
import { SqliteStorage } from "../../src/index.js";

const [dbPath, workerId, mode = "complete"] = process.argv.slice(2);
if (!dbPath || !workerId) throw new Error("usage: claim-worker <dbPath> <workerId> [complete|fail]");

const store = new SqliteStorage({ path: dbPath });
const handled: string[] = [];

for (;;) {
  const job = await store.claim("q", workerId);
  if (!job) break;
  handled.push(job.id);
  if (mode === "fail") await store.fail(job.id, workerId, "boom");
  else await store.complete(job.id, workerId);
}

await store.close();
console.log(JSON.stringify(handled));
