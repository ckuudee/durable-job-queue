// The load-test runner's arithmetic, checked on inputs whose answers are known.
// These functions turn job timestamps into the published numbers, so a mistake
// here would publish a wrong number that looks entirely plausible.
import { describe, expect, it } from "vitest";
import { backlogCurve, lowBacklogRate } from "../bench/scenarios.js";
import { distribution, percentile } from "../bench/lib/stats.js";

/** Completion times for `n` jobs finishing exactly `gapMs` apart. */
const evenly = (n: number, gapMs: number) => Array.from({ length: n }, (_, i) => i * gapMs);

describe("percentile (nearest rank)", () => {
  it("picks the value at the rank, not an interpolation", () => {
    const sorted = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    expect(percentile(sorted, 50)).toBe(5);
    expect(percentile(sorted, 95)).toBe(10);
    expect(percentile(sorted, 10)).toBe(1);
    expect(percentile([7], 99)).toBe(7);
  });

  it("summarizes an unsorted list", () => {
    expect(distribution([5, 1, 4, 2, 3])).toEqual({ p50: 3, p95: 5, p99: 5, max: 5 });
  });
});

describe("lowBacklogRate", () => {
  it("measures only the window where the backlog fell from 2 × low to low", () => {
    // 3,000 jobs: the first 1,000 complete slowly (10 ms apart), the rest fast (1 ms apart).
    const slow = evenly(1000, 10);
    const fast = evenly(2000, 1).map((t) => 10_000 + t);
    // Window: completions 1,000 → 2,000 (backlog 2,000 → 1,000), all fast: 1,000 jobs in 1,000 ms.
    expect(lowBacklogRate([...slow, ...fast], 1000)).toBe(1000);
  });

  it("is null when the drain is too small to contain the window", () => {
    expect(lowBacklogRate(evenly(1999, 1), 1000)).toBeNull();
  });

  it("doesn't depend on the order rows come back in", () => {
    const times = evenly(3000, 2);
    expect(lowBacklogRate([...times].reverse(), 1000)).toBe(lowBacklogRate(times, 1000));
  });
});

describe("backlogCurve", () => {
  it("reports throughput against the backlog still pending", () => {
    // 1,000 jobs, 2 ms apart (500 jobs/s), windows of 100, 1 slot.
    const points = backlogCurve(evenly(1000, 2), 100, 1);
    expect(points[0]).toEqual({ pending: 950, jobsPerSecond: 500 });
    expect(points.map((p) => p.pending)).toEqual([950, 850, 750, 650, 550, 450, 350, 250, 150]);
    expect(points.every((p) => p.jobsPerSecond === 500)).toBe(true);
  });

  it("shows a slowdown at large backlogs", () => {
    // Slow while the backlog is large, fast once it is small.
    const slow = evenly(500, 10);
    const fast = evenly(500, 1).map((t) => 5_000 + t);
    const points = backlogCurve([...slow, ...fast], 100, 1);
    expect(points[0]!.jobsPerSecond).toBe(100);
    expect(points[points.length - 1]!.jobsPerSecond).toBe(1000);
  });

  it("drops the tail where fewer jobs remain than 10× the slot count", () => {
    // 16 slots: windows must end with at least 160 jobs still to go.
    const points = backlogCurve(evenly(1000, 1), 100, 16);
    expect(Math.min(...points.map((p) => p.pending))).toBeGreaterThanOrEqual(160 + 50);
  });
});
