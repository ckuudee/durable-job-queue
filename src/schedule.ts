/**
 * When a newly enqueued job first becomes claimable, from the optional `runAt`
 * (absolute) or `delayMs` (relative to `now`). Neither means "now". A `runAt`
 * in the past is allowed and simply makes the job ready immediately: a producer
 * that was slow to enqueue shouldn't get an error for it.
 */
export function resolveRunAt(now: number, runAt?: number | Date, delayMs?: number): number {
  if (runAt !== undefined && delayMs !== undefined) {
    throw new RangeError("pass runAt or delayMs, not both");
  }
  if (delayMs !== undefined) {
    if (!Number.isSafeInteger(delayMs) || delayMs < 0) {
      throw new RangeError("delayMs must be a non-negative integer");
    }
    return now + delayMs;
  }
  if (runAt !== undefined) {
    const ms = runAt instanceof Date ? runAt.getTime() : runAt;
    // Rejects NaN, which is also what an invalid Date returns from getTime().
    if (!Number.isSafeInteger(ms)) {
      throw new RangeError("runAt must be an integer epoch-milliseconds time or a valid Date");
    }
    return ms;
  }
  return now;
}
