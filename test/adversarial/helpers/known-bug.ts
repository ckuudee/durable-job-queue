import { it, type TestFunction } from "vitest";
import type { Backend } from "./backends.js";

/**
 * A test for a filed bug: `it.fails` on the affected backends (all by default),
 * plain `it` elsewhere. CI stays green while the bug exists, and the test turns
 * red once it's fixed: then drop the wrapper and close the issue.
 */
export function knownBug(issue: number, backend: Backend["name"], onlyOn?: Array<Backend["name"]>) {
  const affected = !onlyOn || onlyOn.includes(backend);
  return (name: string, fn: TestFunction, timeout?: number): void => {
    if (affected) it.fails(`${name} [known bug #${issue}]`, fn, timeout);
    else it(name, fn, timeout);
  };
}
