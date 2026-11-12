import { describe, expect, it } from "vitest";
import { assertIdempotencyKey, canonicalize } from "../src/idempotency.js";

describe("canonicalize", () => {
  it("ignores object key order, at any depth", () => {
    expect(canonicalize({ b: 1, a: { d: 2, c: 3 } })).toBe(canonicalize({ a: { c: 3, d: 2 }, b: 1 }));
  });

  it("keeps array order significant", () => {
    expect(canonicalize([1, 2])).not.toBe(canonicalize([2, 1]));
  });

  it("distinguishes values that look alike", () => {
    const forms = [1, "1", true, null, [], {}, [null], { a: null }].map(canonicalize);
    expect(new Set(forms).size).toBe(forms.length);
  });
});

describe("assertIdempotencyKey", () => {
  it("accepts ordinary keys, up to 255 characters", () => {
    expect(() => assertIdempotencyKey("order-7")).not.toThrow();
    expect(() => assertIdempotencyKey("x".repeat(255))).not.toThrow();
  });

  it("rejects blank and oversized keys", () => {
    expect(() => assertIdempotencyKey("")).toThrow(RangeError);
    expect(() => assertIdempotencyKey("  ")).toThrow(RangeError);
    expect(() => assertIdempotencyKey("x".repeat(256))).toThrow(RangeError);
  });
});
