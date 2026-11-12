// Child process that claims one job and then dies the way a crashed worker does:
// killed outright, with no complete(), no fail() and no store.close().
import { SqliteStorage } from "../../src/index.js";

const [dbPath] = process.argv.slice(2);
if (!dbPath) throw new Error("usage: crash-worker <dbPath>");

const store = new SqliteStorage({ path: dbPath });
const job = await store.claim("q", "crashed-worker");
if (!job) throw new Error("crash-worker found nothing to claim");

process.kill(process.pid, "SIGKILL");
