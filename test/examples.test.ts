import { execFile } from "node:child_process";
import { join } from "node:path";
import { promisify } from "node:util";
import { expect, it } from "vitest";

const run = promisify(execFile);

// The README's usage section is drawn from this example, so running it in CI
// keeps the documentation honest: if the API changes, this test fails.
it("runs the quickstart example end to end", async () => {
  const script = join(import.meta.dirname, "..", "examples", "quickstart.ts");
  const { stdout } = await run(process.execPath, ["--import", "tsx", script]);

  expect(stdout).toContain("the retried enqueue returned the same job: true");
  expect(stdout).toContain('sent "welcome" to ada@example.com');
  expect(stdout).toContain('sent "tips" to ada@example.com');
  expect(stdout).toContain('sent "report" to ops@example.com');
  expect(stdout).toContain("attempt 2 of 2 for nobody@invalid failed");
  expect(stdout).toContain("dead-letter queue holds 1 job(s): mailbox nobody@invalid does not exist");
}, 30_000);
