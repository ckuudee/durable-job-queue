// Property tests for payload round-trips and idempotency-key canonicalization,
// run against every backend: arbitrary JSON in, the same JSON out, and a repeat
// enqueue with the object keys reordered returns the same job.
import fc from "fast-check";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { BACKENDS, type Opened } from "./helpers/backends.js";

/** The same value with every object's keys in reverse insertion order. */
function reorder(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(reorder);
  if (value !== null && typeof value === "object") {
    return Object.fromEntries(Object.entries(value).reverse().map(([k, v]) => [k, reorder(v)]));
  }
  return value;
}

/** What JSON can actually carry: fc.jsonValue() may contain -0, which JSON stores as 0. */
const asJson = (v: unknown): unknown => JSON.parse(JSON.stringify(v));

/** U+0000 or an unpaired surrogate; JSON.stringify escapes them as \u0000 / \udxxx (issue #18). */
const POSTGRES_UNSAFE = /\\u0000|\\ud[89a-f][0-9a-f]{2}/i;
const hasPostgresUnsafeText = (v: unknown): boolean => POSTGRES_UNSAFE.test(JSON.stringify(v));

const RUNS = 60;

describe.each(BACKENDS)("$name: payload properties", (backend) => {
  let opened: Opened;
  let n = 0;
  beforeAll(async () => {
    opened = await backend.open();
  });
  afterAll(async () => {
    await opened.cleanup();
  });

  it("S16/S13: any JSON value round-trips, and a key-reordered repeat returns the same job", async () => {
    await fc.assert(
      fc.asyncProperty(fc.jsonValue(), async (payload) => {
        const key = `k${n++}`;
        const job = await opened.storage.enqueue({ queue: "q", payload, idempotencyKey: key });
        expect((await opened.storage.get(job.id))?.payload).toEqual(asJson(payload));
        const again = await opened.storage.enqueue({ queue: "q", payload: reorder(payload), idempotencyKey: key });
        expect(again.id).toBe(job.id);
      }),
      { numRuns: RUNS },
    );
  }, 60_000);

  // Any Unicode text. U+0000 and lone surrogates are left out: they are known
  // bug #18 on PostgreSQL, pinned down by example tests in
  // idempotency-and-payloads.test.ts. A random generator only hits them
  // sometimes, which would make an it.fails here flaky.
  it("S16/S19: any JSON value with arbitrary Unicode strings round-trips identically on every backend", async () => {
    await fc.assert(
      fc.asyncProperty(fc.jsonValue({ stringUnit: "binary" }).filter((v) => !hasPostgresUnsafeText(v)), async (payload) => {
        const job = await opened.storage.enqueue({ queue: "q", payload });
        expect((await opened.storage.get(job.id))?.payload).toEqual(asJson(payload));
      }),
      { numRuns: RUNS },
    );
  }, 60_000);
});
