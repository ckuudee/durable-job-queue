// Property tests for computeBackoff over every policy validateRetryPolicy
// accepts. Its result becomes `runAt = now + delay`, stored in an INTEGER /
// BIGINT column, so it must always be a finite, non-negative whole number of ms.
import fc from "fast-check";
import { describe, expect, it } from "vitest";
import { computeBackoff, validateRetryPolicy, type RetryPolicy } from "../../src/index.js";

const policy = (opts: { integerFactor?: boolean; positiveBase?: boolean } = {}) =>
  fc
    .record({
      baseDelayMs: fc.integer({ min: opts.positiveBase ? 1 : 0, max: 1_000_000 }),
      factor: opts.integerFactor
        ? fc.integer({ min: 1, max: 10 })
        : fc.double({ min: 1, max: 10, noNaN: true, noDefaultInfinity: true }),
      extra: fc.integer({ min: 0, max: 10_000_000 }),
      jitter: fc.boolean(),
    })
    .map(({ extra, ...p }): RetryPolicy => ({ ...p, maxDelayMs: p.baseDelayMs + extra }))
    .filter((p) => {
      try {
        validateRetryPolicy(p);
        return true;
      } catch {
        return false;
      }
    });

const attempt = fc.integer({ min: 1, max: 5_000 });
const random = fc.double({ min: 0, max: 1, maxExcluded: true, noNaN: true });

function check(p: RetryPolicy, n: number, r: number): void {
  const d = computeBackoff(n, p, () => r);
  expect(Number.isSafeInteger(d), `delay ${d}`).toBe(true);
  expect(d).toBeGreaterThanOrEqual(0);
  expect(d).toBeLessThanOrEqual(p.maxDelayMs);
}

describe("computeBackoff properties", () => {
  it("B1/B2: integer factor, positive base: always a whole number of ms in [0, maxDelayMs], and jitter never goes below half", () => {
    fc.assert(
      fc.property(policy({ integerFactor: true, positiveBase: true }), attempt, random, (p, n, r) => {
        check(p, n, r);
        if (p.jitter) {
          const full = computeBackoff(n, { ...p, jitter: false }, () => r);
          expect(computeBackoff(n, p, () => r)).toBeGreaterThanOrEqual(Math.floor(full / 2));
        }
      }),
    );
  });

  it("B1: without jitter the delay never shrinks from one attempt to the next", () => {
    fc.assert(
      fc.property(policy({ positiveBase: true }), attempt, (p, n) => {
        const q = { ...p, jitter: false };
        expect(computeBackoff(n + 1, q)).toBeGreaterThanOrEqual(computeBackoff(n, q));
      }),
    );
  });

  // Caught #22 (fractional factor without jitter) and #23 (base 0 → 0 * Infinity).
  it("B1: every valid policy gives a storable delay", () => {
    fc.assert(fc.property(policy(), attempt, random, check));
  });
});
