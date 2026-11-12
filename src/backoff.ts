export interface RetryPolicy {
  /** Delay before the first retry, in milliseconds. */
  baseDelayMs: number;
  /** Each further retry waits `factor` times longer than the last. */
  factor: number;
  /** Upper bound on any single delay. */
  maxDelayMs: number;
  /** Randomize delays so many failed jobs don't all retry at the same instant. */
  jitter: boolean;
}

export const DEFAULT_RETRY_POLICY: RetryPolicy = {
  baseDelayMs: 1_000,
  factor: 2,
  maxDelayMs: 60_000,
  jitter: true,
};

export function validateRetryPolicy(policy: RetryPolicy): void {
  if (!(policy.baseDelayMs >= 0)) throw new RangeError("baseDelayMs must be >= 0");
  if (!(policy.factor >= 1)) throw new RangeError("factor must be >= 1");
  if (!(policy.maxDelayMs >= policy.baseDelayMs)) {
    throw new RangeError("maxDelayMs must be >= baseDelayMs");
  }
}

/**
 * Delay (ms) before retrying after the `attempt`-th failed attempt (1-based):
 * exponential growth, capped at `maxDelayMs`. With jitter ("equal jitter") the
 * result lands in [delay / 2, delay], so retries spread out but never fire
 * immediately.
 */
export function computeBackoff(
  attempt: number,
  policy: RetryPolicy,
  random: () => number = Math.random,
): number {
  if (!Number.isInteger(attempt) || attempt < 1) {
    throw new RangeError("attempt must be a positive integer");
  }
  const delay = Math.min(policy.maxDelayMs, policy.baseDelayMs * policy.factor ** (attempt - 1));
  return policy.jitter ? Math.floor(delay / 2 + random() * (delay / 2)) : delay;
}
