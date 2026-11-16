import { createHash } from "node:crypto";
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";
import { PAGE_CSS, PAGE_JS, renderPage } from "./dashboard-page.js";
import type { Storage } from "./storage.js";

export interface DashboardOptions {
  /** Port to listen on; 0 picks a free one. Defaults to 4000. */
  port?: number;
  /**
   * Interface to bind. Defaults to 127.0.0.1, so the dashboard is reachable
   * only from this machine. The dashboard has no authentication: before binding
   * anything else, put it behind a proxy that adds auth, and set `allowedHosts`.
   */
  host?: string;
  /**
   * Host names the browser may use to reach the dashboard (the Host header,
   * without the port). Defaults to localhost, 127.0.0.1, [::1] and `host`. This blocks
   * DNS rebinding: a malicious site pointing its own domain at 127.0.0.1 to
   * read the API from a visitor's browser.
   */
  allowedHosts?: string[];
  /** Dead jobs listed per queue. Defaults to 50. */
  deadLimit?: number;
  /** Called with errors from storage while serving a request. Defaults to console.error. */
  onError?: (err: unknown) => void;
}

export interface Dashboard {
  /** Where to open the dashboard, e.g. http://127.0.0.1:4000/ */
  url: string;
  port: number;
  /** Stop listening and drop open connections, including requests still waiting on storage. */
  close(): Promise<void>;
}

const LOOPBACK_HOSTS = ["localhost", "127.0.0.1", "[::1]"];

/** The dead-letter fields the page shows. Payloads are left out: they may hold personal data. */
interface DeadJobView {
  id: string;
  attempts: number;
  maxAttempts: number;
  error: string | null;
  finishedAt: number | null;
}

const sha256 = (text: string): string => `'sha256-${createHash("sha256").update(text).digest("base64")}'`;

// Only the page's own inline <style> and <script> may run: nothing injected
// into the page can execute, and the page can only fetch from this server.
const CONTENT_SECURITY_POLICY = [
  "default-src 'none'",
  `script-src ${sha256(PAGE_JS)}`,
  `style-src ${sha256(PAGE_CSS)}`,
  "connect-src 'self'",
  "base-uri 'none'",
  "form-action 'none'",
  "frame-ancestors 'none'",
].join("; ");

const PAGE = renderPage();

class HttpError extends Error {
  constructor(
    readonly status: number,
    message: string,
    readonly headers: Record<string, string> = {},
  ) {
    super(message);
  }
}

function send(res: ServerResponse, status: number, contentType: string, body: string, headers: Record<string, string> = {}): void {
  res.writeHead(status, {
    "Content-Type": contentType,
    "Content-Length": Buffer.byteLength(body),
    "Cache-Control": "no-store",
    "X-Content-Type-Options": "nosniff",
    ...headers,
  });
  res.end(body);
}

const sendJson = (res: ServerResponse, status: number, value: unknown, headers?: Record<string, string>): void =>
  send(res, status, "application/json; charset=utf-8", JSON.stringify(value), headers);

/** The Host header's name part: "127.0.0.1:4000" -> "127.0.0.1", "[::1]:4000" -> "[::1]". */
function hostName(header: string | undefined): string | null {
  if (!header) return null;
  try {
    return new URL(`http://${header}`).hostname.toLowerCase();
  } catch {
    return null;
  }
}

/** A URL for humans: wildcard binds are reachable on loopback, and IPv6 needs brackets. */
function displayHost(host: string): string {
  if (host === "0.0.0.0" || host === "::") return "127.0.0.1";
  return host.includes(":") ? `[${host}]` : host;
}

/**
 * Serve a read-only web dashboard for `storage`: per-queue counts from
 * `stats()`, throughput, and the dead-letter queue.
 *
 * ```ts
 * const dashboard = await startDashboard(storage, { port: 4000 });
 * console.log(`open ${dashboard.url}`);
 * ```
 */
export async function startDashboard(storage: Storage, options: DashboardOptions = {}): Promise<Dashboard> {
  const {
    port = 4000,
    host = "127.0.0.1",
    allowedHosts = [...LOOPBACK_HOSTS, displayHost(host)],
    deadLimit = 50,
    onError = (err) => console.error(err),
  } = options;
  if (!Number.isInteger(port) || port < 0 || port > 65_535) throw new RangeError("port must be an integer from 0 to 65535");
  if (!Number.isSafeInteger(deadLimit) || deadLimit < 1) throw new RangeError("deadLimit must be a positive integer");
  const allowed = new Set(allowedHosts.map((h) => h.toLowerCase()));

  async function handle(req: IncomingMessage, res: ServerResponse): Promise<void> {
    const name = hostName(req.headers.host);
    if (name === null || !allowed.has(name)) throw new HttpError(403, "host not allowed");
    if (req.method !== "GET") throw new HttpError(405, "method not allowed", { Allow: "GET" });

    const url = new URL(req.url ?? "/", "http://dashboard.invalid");
    switch (url.pathname) {
      case "/":
        send(res, 200, "text/html; charset=utf-8", PAGE, {
          "Content-Security-Policy": CONTENT_SECURITY_POLICY,
          "Referrer-Policy": "no-referrer",
        });
        return;
      case "/api/stats":
        sendJson(res, 200, { queues: await storage.stats() });
        return;
      case "/api/dead": {
        const queue = url.searchParams.get("queue");
        if (!queue) throw new HttpError(400, "the queue parameter is required");
        const jobs: DeadJobView[] = (await storage.listDead(queue, deadLimit)).map((job) => ({
          id: job.id,
          attempts: job.attempts,
          maxAttempts: job.maxAttempts,
          error: job.error,
          finishedAt: job.finishedAt,
        }));
        sendJson(res, 200, { jobs });
        return;
      }
      default:
        throw new HttpError(404, "not found");
    }
  }

  const server = createServer((req, res) => {
    handle(req, res).catch((err: unknown) => {
      if (err instanceof HttpError) {
        sendJson(res, err.status, { error: err.message }, err.headers);
        return;
      }
      // A storage failure. The details go to onError, not to the browser.
      onError(err);
      if (res.headersSent) res.destroy();
      else sendJson(res, 500, { error: "storage error" });
    });
  });

  await new Promise<void>((resolve, reject) => {
    server.once("error", reject); // e.g. EADDRINUSE
    server.listen(port, host, () => {
      server.off("error", reject);
      resolve();
    });
  });
  const actualPort = (server.address() as AddressInfo).port;

  return {
    url: `http://${displayHost(host)}:${actualPort}/`,
    port: actualPort,
    close: () =>
      new Promise<void>((resolve, reject) => {
        server.close((err) => (err ? reject(err) : resolve()));
        // close() drops idle keep-alive connections (Node 19+) but waits for
        // requests in flight, such as a stats() call stuck on an unreachable
        // database. Shutdown shouldn't hang on those, so drop them too.
        server.closeAllConnections();
      }),
  };
}
