// Dashboard HTTP edge cases: Host header variants (DNS-rebinding guard), IPv6,
// odd paths and methods, query strings. Uses raw http.request so the test, not
// fetch(), controls the Host header and method.
import { request } from "node:http";
import { afterEach, describe, expect, it } from "vitest";
import { startDashboard, type Dashboard, type Storage } from "../../src/index.js";
import { BACKENDS, FakeClock, type Opened } from "./helpers/backends.js";

interface Res {
  status: number;
  headers: Record<string, string | string[] | undefined>;
  body: string;
}

function hit(port: number, opts: { path?: string; method?: string; host?: string | null; address?: string }): Promise<Res> {
  return new Promise((resolve, reject) => {
    const headers: Record<string, string> = {};
    if (opts.host !== null) headers.host = opts.host ?? `127.0.0.1:${port}`;
    const req = request(
      { host: opts.address ?? "127.0.0.1", port, path: opts.path ?? "/api/stats", method: opts.method ?? "GET", headers, setHost: false },
      (res) => {
        let body = "";
        res.on("data", (d) => (body += d));
        res.on("end", () => resolve({ status: res.statusCode!, headers: res.headers, body }));
      },
    );
    req.on("error", reject);
    req.end();
  });
}

describe.each(BACKENDS)("$name: dashboard HTTP", (backend) => {
  let opened: Opened | undefined;
  let dash: Dashboard | undefined;
  afterEach(async () => {
    await dash?.close();
    dash = undefined;
    await opened?.cleanup();
    opened = undefined;
  });
  async function start(options: Parameters<typeof startDashboard>[1] = {}, storage?: Storage) {
    // A frozen clock: stats() includes `at` and `oldestReadyAgeMs`, which would
    // otherwise differ between the server's call and the test's.
    opened = await backend.open({ now: new FakeClock().now });
    await opened.storage.enqueue({ queue: "q", payload: { secret: "ada@example.com" }, maxAttempts: 1 });
    dash = await startDashboard(storage ?? opened.storage, { port: 0, onError: () => {}, ...options });
    return dash;
  }

  it("D1: /api/stats matches storage.stats(), and a query string doesn't change the route", async () => {
    const d = await start();
    const res = await hit(d.port, { path: "/api/stats?queue=nope&x=%00" });
    expect(res.status).toBe(200);
    expect(JSON.parse(res.body)).toEqual({ queues: JSON.parse(JSON.stringify(await opened!.storage.stats())) });
  });

  it.each([
    ["localhost", 200],
    ["LOCALHOST", 200],
    ["127.0.0.1", 200],
    ["[::1]", 200],
    ["[0:0:0:0:0:0:0:1]", 200], // same address, long form
    ["evil.example", 403],
    ["localhost.evil.example", 403],
    ["127.0.0.1.nip.io", 403],
    ["evil.example#@localhost", 403],
    ["localhost@evil.example", 403],
    ["", 403],
  ])("D3: Host %j (with port) -> %i", async (name, expected) => {
    const d = await start();
    const res = await hit(d.port, { host: name === "" ? "" : `${name}:${d.port}` });
    expect(res.status).toBe(expected);
  });

  it("D3: Host without a port is accepted; a missing Host header is refused", async () => {
    const d = await start();
    expect((await hit(d.port, { host: "localhost" })).status).toBe(200);
    expect([400, 403]).toContain((await hit(d.port, { host: null })).status);
  });

  it.each([
    ["POST", 405],
    ["PUT", 405],
    ["DELETE", 405],
    ["PATCH", 405],
    ["OPTIONS", 405],
    ["HEAD", 405],
  ])("D2: %s /api/stats -> %i with Allow: GET, and storage is not touched", async (method, expected) => {
    const d = await start();
    const before = await opened!.storage.stats();
    const res = await hit(d.port, { method });
    expect(res.status).toBe(expected);
    expect(res.headers.allow).toBe("GET");
    expect(await opened!.storage.stats()).toEqual(before);
  });

  it.each(["/api/stats/", "/API/STATS", "/api", "/index.html", "/api/stats/../../etc/passwd", "//api/stats", "/%2e%2e/secret"])(
    "D2: GET %s is 404 and never leaks a stack trace",
    async (path) => {
      const d = await start();
      const res = await hit(d.port, { path });
      expect(res.status).toBe(404);
      expect(res.body).not.toMatch(/at .*\.ts:\d+/);
    },
  );

  it("D3/D4: bound to ::1, the IPv6 URL works and its bracketed Host is allowed", async () => {
    opened = await backend.open();
    try {
      dash = await startDashboard(opened.storage, { port: 0, host: "::1", onError: () => {} });
    } catch {
      return; // no IPv6 loopback on this machine
    }
    expect(dash.url).toBe(`http://[::1]:${dash.port}/`);
    const res = await hit(dash.port, { address: "::1", host: `[::1]:${dash.port}`, path: "/" });
    expect(res.status).toBe(200);
    expect(res.headers["content-type"]).toMatch(/text\/html/);
  });
});
