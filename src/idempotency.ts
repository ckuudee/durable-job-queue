export const MAX_IDEMPOTENCY_KEY_LENGTH = 255;

export function assertIdempotencyKey(key: string): void {
  if (key.trim() === "" || key.length > MAX_IDEMPOTENCY_KEY_LENGTH) {
    throw new RangeError(`idempotencyKey must be 1 to ${MAX_IDEMPOTENCY_KEY_LENGTH} characters, not blank`);
  }
}

/**
 * Deterministic text for a parsed JSON value: object keys are sorted, so
 * `{a: 1, b: 2}` and `{b: 2, a: 1}` compare equal. Used to decide whether a
 * repeated enqueue carries "the same" payload.
 */
export function canonicalize(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonicalize).join(",")}]`;
  if (value !== null && typeof value === "object") {
    const entries = Object.entries(value as Record<string, unknown>)
      .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
      .map(([k, v]) => `${JSON.stringify(k)}:${canonicalize(v)}`);
    return `{${entries.join(",")}}`;
  }
  return JSON.stringify(value);
}
