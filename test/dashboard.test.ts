import { createHash } from "node:crypto";
import { request } from "node:http";
import { afterEach, describe, expect, it } from "vitest";
import { SqliteStorage, startDashboard, type DashboardOptions, type Storage } from "../src/index.js";

const START = 1_000_000;

interface Reply {
  status: number;
  headers: Record<string, string | string[] | undefined>;
  body: string;
}

/**
 * A raw HTTP request. node:http rather than fetch, because fetch won't let a
 * test send a forged Host header, which the DNS-rebinding test needs.
 */
function get(port: number, path: string, { method = "GET", host = `127.0.0.1:${port}` } = {}): Promise<Reply> {
  return new Promise((resolve, reject) => {
    const req = request({ host: "127.0.0.1", port, path, method, headers: { Host: host } }, (res) => {
      let body = "";
      res.setEncoding("utf8");
      res.on("data", (chunk: string) => (body += chunk));
      res.on("end", () => resolve({ status: res.statusCode ?? 0, headers: res.headers, body }));
    });
    req.on("error", reject);
    req.end();
  });
}

const cleanup: Array<() => Promise<void>> = [];
afterEach(async () => {
  for (const fn of cleanup.splice(0).reverse()) await fn();
});

async function setup(options: DashboardOptions = {}, wrap: (s: Storage) => Storage = (s) => s) {
  const clock = { t: START };
  const storage = new SqliteStorage({ path: ":memory:", now: () => clock.t });
  cleanup.push(() => storage.close());
  const errors: unknown[] = [];
  const dashboard = await startDashboard(wrap(storage), { port: 0, onError: (e) => errors.push(e), ...options });
  cleanup.push(() => dashboard.close());
  return { storage, dashboard, clock, errors };
}

describe("dashboard", () => {
  it("listens on 127.0.0.1 by default and reports its URL", async () => {
    const { dashboard } = await setup();
    expect(dashboard.port).toBeGreaterThan(0);
    expect(dashboard.url).toBe(`http://127.0.0.1:${dashboard.port}/`);
  });

  it("serves storage.stats() at /api/stats", async () => {
    const { storage, dashboard, clock } = await setup();
    await storage.enqueue({ queue: "email", payload: 1 });
    await storage.enqueue({ queue: "billing", payload: 2, delayMs: 5_000 });
    clock.t += 1_000;

    const res = await get(dashboard.port, "/api/stats");
    expect(res.status).toBe(200);
    expect(res.headers["content-type"]).toBe("application/json; charset=utf-8");
    expect(res.headers["cache-control"]).toBe("no-store");
    expect(JSON.parse(res.body)).toEqual({ queues: await storage.stats() });
  });

  it("lists dead jobs for a queue without their payloads", async () => {
    const { storage, dashboard } = await setup();
    const { id } = await storage.enqueue({ queue: "q", payload: { email: "a@example.com" }, maxAttempts: 1 });
    await storage.claim("q", "w1");
    await storage.fail(id, "w1", "mailbox full");

    const res = await get(dashboard.port, "/api/dead?queue=q");
    expect(res.status).toBe(200);
    expect(JSON.parse(res.body)).toEqual({
      jobs: [{ id, attempts: 1, maxAttempts: 1, error: "mailbox full", finishedAt: START }],
    });
    expect(res.body).not.toContain("a@example.com");
  });

  it("caps the dead-letter list at deadLimit", async () => {
    const { storage, dashboard } = await setup({ deadLimit: 2 });
    for (let i = 0; i < 3; i++) {
      const { id } = await storage.enqueue({ queue: "q", payload: i, maxAttempts: 1 });
      await storage.claim("q", "w1");
      await storage.fail(id, "w1", "x");
    }
    expect(JSON.parse((await get(dashboard.port, "/api/dead?queue=q")).body).jobs).toHaveLength(2);
  });

  it("requires the queue parameter for /api/dead", async () => {
    const { dashboard } = await setup();
    const res = await get(dashboard.port, "/api/dead");
    expect(res.status).toBe(400);
    expect(JSON.parse(res.body)).toEqual({ error: "the queue parameter is required" });
  });

  it("serves the page with a CSP that allows exactly its own inline script and style", async () => {
    const { dashboard } = await setup();
    const res = await get(dashboard.port, "/");
    expect(res.status).toBe(200);
    expect(res.headers["content-type"]).toBe("text/html; charset=utf-8");
    expect(res.body).toContain("<title>Job queue dashboard</title>");

    // If the page's script and the CSP hash ever drift apart, the browser
    // silently refuses to run the script and the page sits on "Loading".
    const csp = String(res.headers["content-security-policy"]);
    const hash = (text: string) => `'sha256-${createHash("sha256").update(text).digest("base64")}'`;
    const script = /<script>([\s\S]*?)<\/script>/.exec(res.body)?.[1];
    const style = /<style>([\s\S]*?)<\/style>/.exec(res.body)?.[1];
    expect(script).toBeDefined();
    expect(style).toBeDefined();
    expect(csp).toContain(`script-src ${hash(script as string)}`);
    expect(csp).toContain(`style-src ${hash(style as string)}`);
    expect(csp).toContain("default-src 'none'");
    expect(csp).not.toContain("unsafe-inline");
  });

  it("never inserts data into the page as HTML", async () => {
    const { dashboard } = await setup();
    const { body } = await get(dashboard.port, "/");
    // Queue names and errors come from user code; the client must use textContent.
    expect(body).not.toMatch(/innerHTML|outerHTML|insertAdjacentHTML|document\.write/);
  });

  it("returns 404 for unknown paths and 405 for anything but GET", async () => {
    const { dashboard } = await setup();
    expect((await get(dashboard.port, "/admin")).status).toBe(404);
    const post = await get(dashboard.port, "/api/stats", { method: "POST" });
    expect(post.status).toBe(405);
    expect(post.headers.allow).toBe("GET");
  });

  it("rejects requests whose Host header isn't allowed (DNS rebinding)", async () => {
    const { dashboard } = await setup();
    const forged = await get(dashboard.port, "/api/stats", { host: `evil.example:${dashboard.port}` });
    expect(forged.status).toBe(403);
    expect(forged.body).not.toContain("queues");

    for (const host of [`localhost:${dashboard.port}`, `127.0.0.1:${dashboard.port}`, `[::1]:${dashboard.port}`]) {
      expect((await get(dashboard.port, "/api/stats", { host })).status).toBe(200);
    }
  });

  it("accepts extra host names through allowedHosts", async () => {
    const { dashboard } = await setup({ allowedHosts: ["jobs.internal"] });
    expect((await get(dashboard.port, "/api/stats", { host: "jobs.internal" })).status).toBe(200);
    expect((await get(dashboard.port, "/api/stats", { host: `localhost:${dashboard.port}` })).status).toBe(403);
  });

  it("answers 500 without details when storage fails, and reports the error", async () => {
    const secret = new Error("password authentication failed for user jobq");
    const { dashboard, errors } = await setup({}, (s) =>
      Object.assign(Object.create(s) as Storage, {
        stats: async () => {
          throw secret;
        },
      }),
    );
    const res = await get(dashboard.port, "/api/stats");
    expect(res.status).toBe(500);
    expect(JSON.parse(res.body)).toEqual({ error: "storage error" });
    expect(errors).toEqual([secret]);
  });

  it("closes promptly even while a request is stuck waiting on storage", async () => {
    let reached!: () => void;
    const stuck = new Promise<void>((resolve) => (reached = resolve));
    const { dashboard } = await setup({}, (s) =>
      Object.assign(Object.create(s) as Storage, {
        stats: () => {
          reached();
          return new Promise(() => {}); // a database that never answers
        },
      }),
    );
    const pending = get(dashboard.port, "/api/stats").catch((err: unknown) => err);
    await stuck;

    const before = Date.now();
    await dashboard.close();
    expect(Date.now() - before).toBeLessThan(1_000);
    expect(await pending).toBeInstanceOf(Error); // the client's connection was dropped
    cleanup.pop(); // already closed
  });

  it("rejects an invalid port or deadLimit, and a port already in use", async () => {
    const storage = new SqliteStorage({ path: ":memory:" });
    cleanup.push(() => storage.close());
    await expect(startDashboard(storage, { port: -1 })).rejects.toThrow(RangeError);
    await expect(startDashboard(storage, { port: 1.5 })).rejects.toThrow(RangeError);
    await expect(startDashboard(storage, { port: 0, deadLimit: 0 })).rejects.toThrow(RangeError);

    const first = await startDashboard(storage, { port: 0 });
    cleanup.push(() => first.close());
    await expect(startDashboard(storage, { port: first.port })).rejects.toThrow(/EADDRINUSE/);
  });
});
