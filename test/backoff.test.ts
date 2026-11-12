import { describe, expect, it } from "vitest";
import { DEFAULT_RETRY_POLICY, computeBackoff, validateRetryPolicy, type RetryPolicy } from "../src/index.js";

const plain: RetryPolicy = { baseDelayMs: 1_000, factor: 2, maxDelayMs: 10_000, jitter: false };

describe("computeBackoff", () => {
  it("grows exponentially from the base delay", () => {
    expect([1, 2, 3, 4].map((n) => computeBackoff(n, plain))).toEqual([1_000, 2_000, 4_000, 8_000]);
  });

  it("never exceeds maxDelayMs, even for huge attempt numbers", () => {
    expect(computeBackoff(5, plain)).toBe(10_000);
    expect(computeBackoff(10_000, plain)).toBe(10_000);
  });

  it("keeps jittered delays within [delay / 2, delay]", () => {
    const jittery = { ...plain, jitter: true };
    expect(computeBackoff(3, jittery, () => 0)).toBe(2_000); // lower bound
    expect(computeBackoff(3, jittery, () => 0.999999)).toBe(3_999); // just under upper bound
    expect(computeBackoff(3, jittery, () => 0.5)).toBe(3_000);
  });

  it("rejects attempt numbers below 1 or non-integers", () => {
    expect(() => computeBackoff(0, plain)).toThrow(RangeError);
    expect(() => computeBackoff(1.5, plain)).toThrow(RangeError);
  });
});

describe("validateRetryPolicy", () => {
  it("accepts the default policy", () => {
    expect(() => validateRetryPolicy(DEFAULT_RETRY_POLICY)).not.toThrow();
  });

  it("rejects negative delays, shrinking factors and a cap below the base", () => {
    expect(() => validateRetryPolicy({ ...plain, baseDelayMs: -1 })).toThrow(RangeError);
    expect(() => validateRetryPolicy({ ...plain, factor: 0.5 })).toThrow(RangeError);
    expect(() => validateRetryPolicy({ ...plain, maxDelayMs: 500 })).toThrow(RangeError);
  });
});
