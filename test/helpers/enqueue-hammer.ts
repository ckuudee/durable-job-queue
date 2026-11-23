// Child process for the lock-contention test (#45). Enqueues jobs in a tight
// loop on its own connection, prints "ready" after its first write so the test
// knows the lock is contended, and exits when stdin closes. Prints a JSON
// summary on exit: how many writes succeeded and how many threw.
import { SqliteStorage } from "../../src/index.js";

const [dbPath] = process.argv.slice(2);
if (!dbPath) throw new Error("usage: enqueue-hammer <dbPath>");

const store = new SqliteStorage({ path: dbPath });
let stop = false;
process.stdin.on("end", () => { stop = true; });
process.stdin.resume();

let written = 0;
let failed = 0;
while (!stop) {
  try {
    await store.enqueue({ queue: "hammer", payload: written });
    if (++written === 1) process.stdout.write("ready\n");
  } catch {
    failed++;
  }
  // The await above already yields between writes when the store retries, but
  // a write that succeeds first time never yields to the stdin "end" event.
  if (written % 100 === 0) await new Promise((resolve) => setImmediate(resolve));
}

await store.close();
process.stdout.write(`${JSON.stringify({ written, failed })}\n`);
