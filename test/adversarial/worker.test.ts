// Worker stop/start races, lease keeping, and the events contract
// ("every job:claimed is followed by exactly one terminal event") when storage
// misbehaves. Real timers with short leases; SQLite only where the backend is
// irrelevant to the Worker logic under test, both backends otherwise.
import { afterEach, describe, expect, it } from "vitest";
import { JobStateError, Worker, type Job, type Storage } from "../../src/index.js";
import { BACKENDS, sleep, waitFor, type Opened } from "./helpers/backends.js";
import { knownBug } from "./helpers/known-bug.js";

type Terminal = "completed" | "failed" | "abandoned";

/** Records every event and checks the claimed → exactly-one-terminal contract. */
function recordEvents(worker: Worker) {
  const log: Array<{ event: "claimed" | Terminal; id: string; attempts: number }> = [];
  worker.on("job:claimed", ({ job }) => log.push({ event: "claimed", id: job.id, attempts: job.attempts }));
  for (const event of ["completed", "failed", "abandoned"] as const) {
    worker.on(`job:${event}`, ({ job }: { job: Job }) => log.push({ event, id: job.id, attempts: job.attempts }));
  }
  /** Violations of W1: claims with zero or several terminal events, or terminals without a claim. */
  const violations = (): string[] => {
    const perClaim = new Map<string, Terminal[]>();
    const problems: string[] = [];
    for (const e of log) {
      const key = `${e.id}#${e.attempts}`;
      if (e.event === "claimed") {
        if (perClaim.has(key)) problems.push(`${key} claimed twice`);
        perClaim.set(key, []);
      } else if (!perClaim.has(key)) problems.push(`${key} ${e.event} without claim`);
      else perClaim.get(key)!.push(e.event);
    }
    for (const [key, terminals] of perClaim) if (terminals.length !== 1) problems.push(`${key}: [${terminals.join(", ")}]`);
    return problems;
  };
  return { log, violations };
}

/** Storage proxy whose methods can be made to fail or hang per call. */
function faulty(inner: Storage) {
  const faults: Partial<Record<keyof Storage, (...args: unknown[]) => Promise<unknown> | undefined>> = {};
  const proxy = new Proxy(inner, {
    get(target, prop, receiver) {
      const original = Reflect.get(target, prop, receiver);
      if (typeof original !== "function") return original;
      return (...args: unknown[]) => {
        const fault = faults[prop as keyof Storage]?.(...args);
        return fault ?? original.apply(target, args);
      };
    },
  });
  return { storage: proxy as Storage, faults };
}

describe.each(BACKENDS)("$name: Worker", (backend) => {
  let opened: Opened | undefined;
  const workers: Worker[] = [];
  afterEach(async () => {
    for (const w of workers.splice(0)) await w.stop({ timeoutMs: 0 }).catch(() => {});
    await opened?.cleanup();
    opened = undefined;
  });
  function worker(storage: Storage, options: ConstructorParameters<typeof Worker>[1]): Worker {
    const w = new Worker(storage, { pollIntervalMs: 5, onError: () => {}, ...options });
    workers.push(w);
    return w;
  }

  it("W7: after stop({timeoutMs}) gives up on a handler that ignores its signal, the job's lease lapses and another worker can run it", async () => {
    opened = await backend.open();
    const s = opened.storage;
    const { id } = await s.enqueue({ queue: "q", payload: 1, leaseMs: 150 });
    let unstick!: () => void;
    // Ignores its signal, like a handler stuck in a call that can't be aborted.
    const w = worker(s, { queue: "q", handler: () => new Promise<void>((r) => (unstick = r)) });
    w.start();
    await waitFor(async () => (await s.get(id))?.status === "running", 5_000, "claim");
    await w.stop({ timeoutMs: 20 });
    try {
      // README: "their leases lapse and another worker runs them again".
      await sleep(600); // four lease lengths
      const job = await s.get(id);
      expect(job!.leaseUntil!).toBeLessThanOrEqual(Date.now());
      const reclaimed = await s.claim("q", "other");
      expect(reclaimed?.id).toBe(id);
    } finally {
      unstick(); // let run() finish before cleanup closes the store
      await sleep(10);
    }
  });

  knownBug(21, backend.name)("W6: a second concurrent stop() also waits for in-flight jobs before resolving", async () => {
    opened = await backend.open();
    const s = opened.storage;
    await s.enqueue({ queue: "q", payload: 1 });
    let release!: () => void;
    let finished = false;
    const w = worker(s, {
      queue: "q",
      handler: async () => {
        await new Promise<void>((r) => (release = r));
        finished = true;
      },
    });
    w.start();
    await waitFor(() => release !== undefined, 5_000, "handler start");
    const first = w.stop();
    const second = w.stop(); // e.g. SIGTERM and SIGINT handlers both shutting down
    let secondDone = false;
    void second.then(() => (secondDone = true));
    await sleep(50);
    // The caller of the second stop() would now close storage under a running job.
    expect({ secondDone, finished }).toEqual({ secondDone: false, finished: false });
    release();
    await Promise.all([first, second]);
  });

  it("W9: heartbeats keep a handler that runs several lease lengths from being taken over", async () => {
    opened = await backend.open();
    const s = opened.storage;
    const { id } = await s.enqueue({ queue: "q", payload: 1, leaseMs: 90 });
    const w = worker(s, { queue: "q", handler: () => sleep(400) });
    const ev = recordEvents(w);
    w.start();
    await waitFor(async () => (await s.get(id))?.status === "running", 5_000, "claim");
    // A competing claimer keeps trying the whole time.
    let stolen = false;
    while ((await s.get(id))?.status === "running") {
      if (await s.claim("q", "thief")) stolen = true;
      await sleep(5);
    }
    expect(stolen).toBe(false);
    expect((await s.get(id))?.status).toBe("completed");
    expect(ev.violations()).toEqual([]);
  });

  it("W1/W3: with complete(), fail() and heartbeat() randomly rejecting, every claim still gets exactly one terminal event", async () => {
    opened = await backend.open({ retry: { baseDelayMs: 1, maxDelayMs: 5 } });
    const { storage, faults } = faulty(opened.storage);
    let n = 0;
    const flaky = () => (++n % 3 === 0 ? Promise.reject(new Error("db blip")) : undefined);
    faults.complete = flaky;
    faults.fail = flaky;
    faults.heartbeat = flaky;
    for (let i = 0; i < 30; i++) await opened.storage.enqueue({ queue: "q", payload: i, leaseMs: 60, maxAttempts: 10 });
    const w = worker(storage, {
      queue: "q",
      concurrency: 4,
      handler: async (job) => {
        await sleep(25);
        if ((job.payload as number) % 2 === 0 && job.attempts === 1) throw new Error("first try fails");
      },
    });
    const ev = recordEvents(w);
    w.start();
    await waitFor(async () => (await opened!.storage.stats("q"))[0]?.completed === 30, 20_000, "all done");
    await w.stop();
    expect(ev.violations()).toEqual([]);
  }, 30_000);

  it("W1/W2: a lost lease mid-handler emits abandoned (not completed), aborts the signal, and the event carries the job as claimed", async () => {
    opened = await backend.open();
    const { storage, faults } = faulty(opened.storage);
    const { id } = await opened.storage.enqueue({ queue: "q", payload: 1, leaseMs: 60 });
    let sawAbort = false;
    const w = worker(storage, {
      queue: "q",
      handler: async (_job, { signal }) => {
        await new Promise<void>((resolve) => signal.addEventListener("abort", () => resolve(), { once: true }));
        sawAbort = true;
      },
    });
    const ev = recordEvents(w);
    let abandonedJob: Job | undefined;
    w.on("job:abandoned", ({ job }) => (abandonedJob = job));
    faults.heartbeat = () => Promise.reject(new JobStateError("lost"));
    w.start();
    await waitFor(() => sawAbort, 5_000, "abort");
    await w.stop();
    expect(ev.violations()).toEqual([]);
    expect(ev.log.map((e) => e.event)).toEqual(["claimed", "abandoned"]);
    expect(abandonedJob).toMatchObject({ id, status: "running", attempts: 1 });
  });

  it("W1/W7: stop({timeoutMs}) racing a handler that finishes right at the deadline yields exactly one terminal event per claim", async () => {
    opened = await backend.open();
    const s = opened.storage;
    for (let round = 0; round < 15; round++) {
      const { id } = await s.enqueue({ queue: `q${round}`, payload: round });
      const w = worker(s, { queue: `q${round}`, handler: () => sleep(20) });
      const ev = recordEvents(w);
      w.start();
      await waitFor(() => ev.log.length > 0, 5_000, "claim");
      await w.stop({ timeoutMs: 15 + (round % 10) });
      await sleep(40);
      expect(ev.violations(), `round ${round}`).toEqual([]);
      const status = (await s.get(id))?.status;
      const terminal = ev.log[1]?.event;
      // abandoned ⇒ nothing recorded; completed ⇒ recorded
      expect(`${terminal}:${status}`).toMatch(/^(abandoned:running|completed:completed)$/);
    }
  }, 30_000);

  it("W6: stop() then an immediate start() keeps working and never runs one claim twice", async () => {
    opened = await backend.open();
    const s = opened.storage;
    for (let i = 0; i < 30; i++) await s.enqueue({ queue: "q", payload: i });
    const w = worker(s, { queue: "q", concurrency: 2, handler: () => sleep(3) });
    const ev = recordEvents(w);
    w.start();
    for (let i = 0; i < 10; i++) {
      void w.stop();
      w.start();
      await sleep(5);
    }
    await waitFor(async () => (await s.stats("q"))[0]?.completed === 30, 10_000, "all done");
    await w.stop();
    expect(ev.violations()).toEqual([]);
  });

  it("S1/R133: within one Worker (concurrency 2), a job whose lease lapsed during a DB blip is not run by two slots at once", async () => {
    opened = await backend.open();
    const { storage, faults } = faulty(opened.storage);
    const { id } = await opened.storage.enqueue({ queue: "q", payload: 1, leaseMs: 60 });
    let running = 0;
    let overlap = 0;
    let calls = 0;
    faults.heartbeat = () => Promise.reject(new Error("db blip")); // not a JobStateError: the worker keeps going
    const w = worker(storage, {
      queue: "q",
      concurrency: 2,
      handler: async () => {
        calls++;
        if (++running > 1) overlap++;
        await sleep(200); // > leaseMs, heartbeats failing
        running--;
      },
    });
    const ev = recordEvents(w);
    w.start();
    await waitFor(async () => (await opened!.storage.get(id))?.status !== "running" && calls > 0, 5_000, "settle");
    await w.stop();
    // README: "no two workers run the same job at once"; "a worker that lost its lease
    // can no longer complete". The second claim came from the *same* workerId.
    expect({ overlap, events: ev.log.map((e) => `${e.event}#${e.attempts}`) }).toEqual({
      overlap: 0,
      events: expect.any(Array),
    });
  });
});
