import type { Storage } from "../../src/index.js";

/**
 * Claim the next ready job in `queue` and return its lease token, the value
 * heartbeat/complete/fail need. Throws if nothing was ready, so a test that
 * expected a claim fails loudly instead of passing `undefined` along.
 */
export async function claimToken(s: Storage, queue: string, workerId: string): Promise<string> {
  const job = await s.claim(queue, workerId);
  if (!job?.leaseToken) throw new Error(`expected to claim a job from "${queue}"`);
  return job.leaseToken;
}
