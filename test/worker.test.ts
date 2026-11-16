import { afterEach, describe, expect, it } from "vitest";
import {
  JobStateError,
  SqliteStorage,
  Worker,
  type Job,
  type Storage,
  type WorkerEvents,
  type WorkerOptions,
} from "../src/index.js";

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/** Poll until `check` passes, or fail after `timeoutMs`. */
async function waitFor(check: () => boolean | Promise<boolean>, timeoutMs = 3_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!(await check())) {
    if (Date.now() > deadline) throw new Error("timed out waiting for condition");
    await sleep(5);
  }
}

/** Resolves when `release()` is called; lets a test hold a handler mid-job. */
function gate() {
  let release!: () => void;
  const opened = new Promise<void>((resolve) => (release = resolve));
  return { opened, release };
}

const cleanup: Array<() => Promise<void>> = [];
afterEach(async () => {
  for (const fn of cleanup.splice(0).reverse()) await fn();
});

function setup(opts: { leaseMs?: number } = {}) {
  const storage = new SqliteStorage({
    path: ":memory:",
    leaseMs: opts.leaseMs,
    retry: { baseDelayMs: 10, maxDelayMs: 10, jitter: false },
  });
  cleanup.push(() => storage.close());
  const errors: unknown[] = [];
  const worker = (options: Omit<WorkerOptions, "queue"> & { queue?: string }) => {
    const w = new Worker(storage, { queue: "q", pollIntervalMs: 5, onError: (e) => errors.push(e), ...options });
    cleanup.push(() => w.stop({ timeoutMs: 0 }));
    return w;
  };
  return { storage, worker, errors };
}

describe("Worker", () => {
  it("runs the handler for each job and completes it", async () => {
    const { storage, worker, errors } = setup();
    const ids = await Promise.all([1, 2, 3].map((n) => storage.enqueue({ queue: "q", payload: { n } })));
    const seen: unknown[] = [];
    const w = worker({ handler: async (job) => void seen.push(job.payload), workerId: "w1" });

    w.start();
    await waitFor(async () => (await storage.get(ids[2]!.id))?.status === "completed");
    await w.stop();

    expect(seen).toEqual([{ n: 1 }, { n: 2 }, { n: 3 }]);
    for (const { id } of ids) {
      expect(await storage.get(id)).toMatchObject({ status: "completed", workerId: "w1", attempts: 1 });
    }
    expect(errors).toEqual([]);
  });

  it("picks up jobs enqueued while it is idle", async () => {
    const { storage, worker } = setup();
    const done: string[] = [];
    const w = worker({ handler: async (job) => void done.push(job.id) });
    w.start();
    await sleep(20); // several empty polls
    const job = await storage.enqueue({ queue: "q", payload: 1 });
    await waitFor(() => done.includes(job.id));
    await w.stop();
  });

  it("fails a job whose handler throws, retrying it until it is dead", async () => {
    const { storage, worker, errors } = setup();
    const job = await storage.enqueue({ queue: "q", payload: 1, maxAttempts: 3 });
    let calls = 0;
    const w = worker({
      handler: async () => {
        calls++;
        throw new Error(`boom ${calls}`);
      },
    });

    w.start();
    await waitFor(async () => (await storage.get(job.id))?.status === "dead");
    await w.stop();

    expect(calls).toBe(3);
    expect(await storage.get(job.id)).toMatchObject({ status: "dead", attempts: 3, error: "boom 3" });
    expect(errors).toEqual([]); // handler failures fail the job; they aren't worker errors
  });

  it("runs up to `concurrency` jobs at once, and no more", async () => {
    const { storage, worker } = setup();
    for (let i = 0; i < 6; i++) await storage.enqueue({ queue: "q", payload: i });
    let active = 0;
    let peak = 0;
    let finished = 0;
    const w = worker({
      concurrency: 3,
      handler: async () => {
        peak = Math.max(peak, ++active);
        await sleep(30);
        active--;
        finished++;
      },
    });

    w.start();
    await waitFor(() => finished === 6);
    await w.stop();
    expect(peak).toBe(3);
  });

  it("keeps a long job's lease alive with heartbeats", async () => {
    const { storage, worker } = setup({ leaseMs: 150 });
    const job = await storage.enqueue({ queue: "q", payload: 1 });
    const holding = gate();
    const w = worker({ workerId: "w1", handler: () => holding.opened });

    w.start();
    await waitFor(async () => (await storage.get(job.id))?.status === "running");
    await sleep(500); // over three lease lengths
    expect(await storage.claim("q", "thief")).toBeNull(); // still held by w1
    holding.release();

    await waitFor(async () => (await storage.get(job.id))?.status === "completed");
    expect(await storage.get(job.id)).toMatchObject({ workerId: "w1", attempts: 1 });
    await w.stop();
  });

  it("aborts the handler and records nothing when the job is lost", async () => {
    const { storage, errors } = setup({ leaseMs: 30 });
    // A store whose heartbeats report the job as taken, as if our lease lapsed
    // and another worker claimed it.
    let completes = 0;
    const lossy: Storage = Object.assign(Object.create(storage) as Storage, {
      heartbeat: async (id: string) => {
        throw new JobStateError(`job ${id} is not running under this worker`);
      },
      complete: async () => {
        completes++;
      },
    });
    await storage.enqueue({ queue: "q", payload: 1 });
    let reason: unknown;
    const w = new Worker(lossy, {
      queue: "q",
      pollIntervalMs: 5,
      onError: (e) => errors.push(e),
      handler: (_job, { signal }) =>
        new Promise<void>((resolve) => {
          signal.addEventListener("abort", () => {
            reason = signal.reason;
            resolve(); // the handler finished anyway...
          });
        }),
    });
    cleanup.push(() => w.stop({ timeoutMs: 0 }));

    w.start();
    await waitFor(() => reason !== undefined);
    await w.stop();
    expect(reason).toBeInstanceOf(JobStateError);
    expect(completes).toBe(0); // ...but its result was not recorded
    expect(errors[0]).toBeInstanceOf(JobStateError);
  });

  it("finishes in-flight jobs on stop, and claims nothing after", async () => {
    const { storage, worker } = setup();
    const first = await storage.enqueue({ queue: "q", payload: 1 });
    const second = await storage.enqueue({ queue: "q", payload: 2 });
    const holding = gate();
    const w = worker({ handler: () => holding.opened });

    w.start();
    await waitFor(async () => (await storage.get(first.id))?.status === "running");
    const stopped = w.stop();
    holding.release();
    await stopped;

    expect((await storage.get(first.id))?.status).toBe("completed");
    expect((await storage.get(second.id))?.status).toBe("pending");
  });

  it("gives up on stuck jobs after the stop timeout, aborting their signal", async () => {
    const { storage, worker } = setup();
    const job = await storage.enqueue({ queue: "q", payload: 1 });
    let aborted = false;
    const w = worker({
      handler: (_job, { signal }) =>
        new Promise<void>(() => {
          signal.addEventListener("abort", () => (aborted = true)); // never resolves
        }),
    });

    w.start();
    await waitFor(async () => (await storage.get(job.id))?.status === "running");
    const before = Date.now();
    await w.stop({ timeoutMs: 30 });

    expect(Date.now() - before).toBeLessThan(1_000);
    expect(aborted).toBe(true);
    expect((await storage.get(job.id))?.status).toBe("running"); // left for its lease to lapse
  });

  it("survives a claim that throws, reporting it and trying again", async () => {
    const { storage, errors } = setup();
    let failures = 2;
    const flaky: Storage = Object.assign(Object.create(storage) as Storage, {
      claim: async (queue: string, workerId: string): Promise<Job | null> => {
        if (failures-- > 0) throw new Error("connection refused");
        return storage.claim(queue, workerId);
      },
    });
    const job = await storage.enqueue({ queue: "q", payload: 1 });
    const w = new Worker(flaky, { queue: "q", pollIntervalMs: 5, onError: (e) => errors.push(e), handler: async () => {} });
    cleanup.push(() => w.stop());

    w.start();
    await waitFor(async () => (await storage.get(job.id))?.status === "completed");
    expect(errors.map((e) => (e as Error).message)).toEqual(["connection refused", "connection refused"]);
  });

  it("stops promptly while idle instead of finishing its poll delay", async () => {
    const { worker } = setup();
    const w = worker({ pollIntervalMs: 60_000, handler: async () => {} });
    w.start();
    await sleep(10); // now asleep for up to a minute
    const before = Date.now();
    await w.stop();
    expect(Date.now() - before).toBeLessThan(500);
  });

  it("ignores a second start, and a stop before start", async () => {
    const { storage, worker } = setup();
    let calls = 0;
    const w = worker({ handler: async () => void calls++ });
    await w.stop();
    w.start();
    w.start();
    const job = await storage.enqueue({ queue: "q", payload: 1 });
    await waitFor(async () => (await storage.get(job.id))?.status === "completed");
    await w.stop();
    expect(calls).toBe(1);
  });

  it("rejects invalid options", () => {
    const { storage } = setup();
    const handler = async () => {};
    expect(() => new Worker(storage, { queue: " ", handler })).toThrow(RangeError);
    expect(() => new Worker(storage, { queue: "q", handler, concurrency: 0 })).toThrow(RangeError);
    expect(() => new Worker(storage, { queue: "q", handler, pollIntervalMs: 1.5 })).toThrow(RangeError);
    expect(() => new Worker(storage, { queue: "q", handler, workerId: "" })).toThrow(RangeError);
  });
});

type Recorded = { [K in keyof WorkerEvents]: [K, WorkerEvents[K][0]] }[keyof WorkerEvents];

/** Every lifecycle event the worker emits, in order. */
function record(w: Worker): Recorded[] {
  const events: Recorded[] = [];
  w.on("job:claimed", (e) => events.push(["job:claimed", e]));
  w.on("job:completed", (e) => events.push(["job:completed", e]));
  w.on("job:failed", (e) => events.push(["job:failed", e]));
  w.on("job:abandoned", (e) => events.push(["job:abandoned", e]));
  return events;
}

const names = (events: Recorded[]) => events.map(([name]) => name);

describe("Worker events", () => {
  it("emits claimed then completed, with the handler's duration", async () => {
    const { storage, worker, errors } = setup();
    const job = await storage.enqueue({ queue: "q", payload: 1 });
    const w = worker({
      handler: async () => {
        await sleep(20);
      },
    });
    const events = record(w);

    w.start();
    await waitFor(() => events.length === 2);
    await w.stop();

    expect(names(events)).toEqual(["job:claimed", "job:completed"]);
    const [, completed] = events[1] as ["job:completed", WorkerEvents["job:completed"][0]];
    expect(completed.job).toMatchObject({ id: job.id, status: "running" }); // the job as claimed
    expect(completed.durationMs).toBeGreaterThanOrEqual(15); // timers can fire a little early
    // Emitted only after the completion is stored, so a listener reading storage sees it.
    expect(await storage.get(job.id)).toMatchObject({ status: "completed" });
    expect(errors).toEqual([]);
  });

  it("emits failed with willRetry until the last attempt, which goes to the dead-letter queue", async () => {
    const { storage, worker } = setup();
    await storage.enqueue({ queue: "q", payload: 1, maxAttempts: 3 });
    const boom = new Error("boom");
    const w = worker({
      handler: async () => {
        throw boom;
      },
    });
    const events = record(w);

    w.start();
    await waitFor(() => events.length === 6);
    await w.stop();

    expect(names(events)).toEqual([
      "job:claimed", "job:failed", "job:claimed", "job:failed", "job:claimed", "job:failed",
    ]);
    const failures = events.filter((e): e is ["job:failed", WorkerEvents["job:failed"][0]] => e[0] === "job:failed");
    expect(failures.map(([, e]) => e.willRetry)).toEqual([true, true, false]);
    expect(failures.every(([, e]) => e.error === boom)).toBe(true); // the thrown value itself, not just its message
  });

  it("emits abandoned, and never completed, when the job is lost to another worker", async () => {
    const { storage, errors } = setup({ leaseMs: 30 });
    const lossy: Storage = Object.assign(Object.create(storage) as Storage, {
      heartbeat: async (id: string) => {
        throw new JobStateError(`job ${id} is not running under this worker`);
      },
    });
    await storage.enqueue({ queue: "q", payload: 1 });
    const w = new Worker(lossy, {
      queue: "q",
      pollIntervalMs: 5,
      onError: (e) => errors.push(e),
      handler: (_job, { signal }) => new Promise<void>((resolve) => signal.addEventListener("abort", () => resolve())),
    });
    cleanup.push(() => w.stop({ timeoutMs: 0 }));
    const events = record(w);

    w.start();
    await waitFor(() => events.length === 2);
    await w.stop();

    expect(names(events)).toEqual(["job:claimed", "job:abandoned"]);
    expect(events[1]?.[1]).toMatchObject({ reason: expect.any(JobStateError) });
  });

  it("emits abandoned as soon as stop() times out, even if the handler never returns", async () => {
    const { storage, worker } = setup();
    await storage.enqueue({ queue: "q", payload: 1 });
    const w = worker({ handler: () => new Promise<void>(() => {}) }); // hangs forever
    const events = record(w);

    w.start();
    await waitFor(() => events.length === 1);
    await w.stop({ timeoutMs: 20 });

    expect(names(events)).toEqual(["job:claimed", "job:abandoned"]);
    expect(events[1]?.[1]).toMatchObject({ reason: expect.objectContaining({ message: expect.stringMatching(/stopped/) }) });
  });

  it("emits abandoned when storage refuses to record the result", async () => {
    const { storage, errors } = setup();
    const down = new Error("connection reset");
    const broken: Storage = Object.assign(Object.create(storage) as Storage, {
      complete: async () => {
        throw down;
      },
    });
    await storage.enqueue({ queue: "q", payload: 1 });
    const w = new Worker(broken, { queue: "q", pollIntervalMs: 5, onError: (e) => errors.push(e), handler: async () => {} });
    cleanup.push(() => w.stop({ timeoutMs: 0 }));
    const events = record(w);

    w.start();
    await waitFor(() => events.length === 2);
    await w.stop({ timeoutMs: 0 });

    expect(names(events)).toEqual(["job:claimed", "job:abandoned"]);
    expect(events[1]?.[1]).toMatchObject({ reason: down });
    expect(errors).toContain(down); // still reported as a worker error, as before
  });

  it("emits exactly one outcome when stop() times out while the result is being recorded", async () => {
    const { storage } = setup();
    const recording = gate();
    const slow: Storage = Object.assign(Object.create(storage) as Storage, {
      complete: async (id: string, workerId: string) => {
        await recording.opened; // hold complete() in flight
        return storage.complete(id, workerId);
      },
    });
    const job = await storage.enqueue({ queue: "q", payload: 1 });
    const w = new Worker(slow, { queue: "q", pollIntervalMs: 5, onError: () => {}, handler: async () => {} });
    cleanup.push(() => w.stop({ timeoutMs: 0 }));
    const events = record(w);

    w.start();
    await waitFor(() => events.length === 1);
    await sleep(10); // the handler is done; complete() is now waiting on the gate
    await w.stop({ timeoutMs: 10 }); // times out and aborts the in-flight job...
    recording.release(); // ...but the completion still lands
    await waitFor(async () => (await storage.get(job.id))?.status === "completed");
    await sleep(10);

    expect(names(events)).toEqual(["job:claimed", "job:completed"]);
  });

  it("isolates a throwing listener: later listeners still run and the job still completes", async () => {
    const { storage, worker, errors } = setup();
    const job = await storage.enqueue({ queue: "q", payload: 1 });
    const w = worker({ handler: async () => {} });
    const bug = new Error("listener bug");
    w.on("job:claimed", () => {
      throw bug;
    });
    w.on("job:completed", () => {
      throw bug;
    });
    const events = record(w); // registered after the throwing listeners

    w.start();
    await waitFor(() => events.length === 2);
    await w.stop();

    expect(names(events)).toEqual(["job:claimed", "job:completed"]);
    expect(await storage.get(job.id)).toMatchObject({ status: "completed" });
    expect(errors).toEqual([bug, bug]);
  });

  it("reports an async listener's rejection to onError instead of crashing the process", async () => {
    const { storage, worker, errors } = setup();
    await storage.enqueue({ queue: "q", payload: 1 });
    const w = worker({ handler: async () => {} });
    const bug = new Error("async listener bug");
    w.on("job:completed", async () => {
      throw bug;
    });

    w.start();
    await waitFor(() => errors.length === 1); // vitest fails the run on any unhandled rejection
    await w.stop();
    expect(errors).toEqual([bug]);
  });
});
