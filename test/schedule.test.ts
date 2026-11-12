import { describe, expect, it } from "vitest";
import { resolveRunAt } from "../src/schedule.js";

describe("resolveRunAt", () => {
  const NOW = 1_000_000;

  it("defaults to now", () => {
    expect(resolveRunAt(NOW)).toBe(NOW);
  });

  it("adds delayMs to now", () => {
    expect(resolveRunAt(NOW, undefined, 250)).toBe(NOW + 250);
  });

  it("accepts runAt as epoch ms or a Date, including times in the past", () => {
    expect(resolveRunAt(NOW, NOW + 5)).toBe(NOW + 5);
    expect(resolveRunAt(NOW, new Date(NOW - 5))).toBe(NOW - 5);
  });

  it("rejects both at once and invalid values", () => {
    expect(() => resolveRunAt(NOW, NOW, 0)).toThrow(/not both/);
    expect(() => resolveRunAt(NOW, undefined, -1)).toThrow(RangeError);
    expect(() => resolveRunAt(NOW, undefined, Number.MAX_SAFE_INTEGER + 1)).toThrow(RangeError);
    expect(() => resolveRunAt(NOW, 1.5)).toThrow(RangeError);
    expect(() => resolveRunAt(NOW, new Date(Number.NaN))).toThrow(RangeError);
  });
});
