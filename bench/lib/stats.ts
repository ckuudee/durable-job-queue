/** Nearest-rank percentile (p in 0..100) of an ascending array. */
export function percentile(sorted: number[], p: number): number {
  if (sorted.length === 0) throw new Error("percentile of an empty list");
  const rank = Math.ceil((p / 100) * sorted.length);
  return sorted[Math.min(sorted.length, Math.max(1, rank)) - 1]!;
}

export interface Distribution {
  p50: number;
  p95: number;
  p99: number;
  max: number;
}

export function distribution(values: number[]): Distribution {
  const sorted = [...values].sort((a, b) => a - b);
  return {
    p50: percentile(sorted, 50),
    p95: percentile(sorted, 95),
    p99: percentile(sorted, 99),
    max: sorted[sorted.length - 1]!,
  };
}

export function median(values: number[]): number {
  return percentile(
    [...values].sort((a, b) => a - b),
    50,
  );
}

/** Median and range of one number across repeated runs. */
export interface Spread {
  median: number;
  min: number;
  max: number;
}

export function spread(values: number[]): Spread {
  return { median: median(values), min: Math.min(...values), max: Math.max(...values) };
}

/** Median of each percentile across repeated runs. */
export function medianDistribution(runs: Distribution[]): Distribution {
  return {
    p50: median(runs.map((d) => d.p50)),
    p95: median(runs.map((d) => d.p95)),
    p99: median(runs.map((d) => d.p99)),
    max: median(runs.map((d) => d.max)),
  };
}
