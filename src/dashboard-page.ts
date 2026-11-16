// The dashboard's single page. CSS and JS are inline so the server has one
// route for the UI; the Content-Security-Policy allows exactly these two blocks
// by hash (see dashboard.ts), so injected markup can't run scripts.
//
// The client script never uses innerHTML with data: queue names and error
// messages come from user code (a handler can throw any string), so every value
// goes in through textContent.

export const PAGE_CSS = `
:root {
  --bg: #fafaf9; --surface: #ffffff; --text: #1c1917; --muted: #78716c;
  --border: #e7e5e4; --accent: #2563eb; --warn: #b45309; --bad: #b91c1c;
  --warn-bg: #fef3c7; --bad-bg: #fee2e2;
}
@media (prefers-color-scheme: dark) {
  :root {
    --bg: #0c0a09; --surface: #1c1917; --text: #f5f5f4; --muted: #a8a29e;
    --border: #292524; --accent: #60a5fa; --warn: #fbbf24; --bad: #f87171;
    --warn-bg: #422006; --bad-bg: #450a0a;
  }
}
* { box-sizing: border-box; }
body {
  margin: 0; padding: 24px 16px; background: var(--bg); color: var(--text);
  font: 14px/1.5 system-ui, -apple-system, "Segoe UI", sans-serif;
}
main { max-width: 1100px; margin: 0 auto; }
header { display: flex; align-items: baseline; justify-content: space-between; gap: 12px; flex-wrap: wrap; }
h1 { font-size: 20px; margin: 0; }
h2 { font-size: 16px; margin: 32px 0 8px; }
#status { color: var(--muted); font-size: 13px; }
#status.error { color: var(--bad); }
.card { background: var(--surface); border: 1px solid var(--border); border-radius: 8px; overflow-x: auto; margin-top: 16px; }
table { width: 100%; border-collapse: collapse; font-variant-numeric: tabular-nums; }
th, td { padding: 8px 12px; text-align: right; white-space: nowrap; border-bottom: 1px solid var(--border); }
th { color: var(--muted); font-weight: 500; font-size: 12px; text-transform: uppercase; letter-spacing: 0.03em; }
th:first-child, td:first-child { text-align: left; }
tr:last-child td { border-bottom: none; }
td.wrap { white-space: normal; text-align: left; word-break: break-word; min-width: 240px; }
button.queue {
  font: inherit; color: var(--accent); background: none; border: none; padding: 0; cursor: pointer; text-align: left;
}
button.queue[aria-pressed="true"] { font-weight: 600; text-decoration: underline; }
.warn { color: var(--warn); background: var(--warn-bg); border-radius: 4px; padding: 1px 6px; }
.bad { color: var(--bad); background: var(--bad-bg); border-radius: 4px; padding: 1px 6px; }
.rate { display: inline-flex; align-items: center; gap: 8px; }
.spark { fill: none; stroke: var(--accent); stroke-width: 1.5; }
.empty { padding: 24px; color: var(--muted); text-align: center; }
.legend { color: var(--muted); font-size: 13px; margin-top: 8px; }
code { font: 12px ui-monospace, SFMono-Regular, Consolas, monospace; }
`;

export const PAGE_JS = `
"use strict";
const POLL_MS = 2000;
const HISTORY = 60; // samples kept per queue: two minutes at one poll every 2 s
const SVG_NS = "http://www.w3.org/2000/svg";
const history = new Map(); // queue -> [{ at, completed }]
let selected = null;

function el(tag, text, className) {
  const node = document.createElement(tag);
  if (text !== undefined && text !== null) node.textContent = String(text);
  if (className) node.className = className;
  return node;
}

function formatAge(ms) {
  if (ms === null) return "\\u2014";
  if (ms < 1000) return ms + " ms";
  const s = ms / 1000;
  if (s < 60) return s.toFixed(1) + " s";
  const m = s / 60;
  if (m < 60) return m.toFixed(1) + " min";
  return (m / 60).toFixed(1) + " h";
}

// Completed jobs per second between consecutive polls. The completed count
// only grows (nothing deletes completed jobs), so a drop means the store was
// reset; that sample counts as zero.
function rates(samples) {
  const out = [];
  for (let i = 1; i < samples.length; i++) {
    const seconds = (samples[i].at - samples[i - 1].at) / 1000;
    const done = samples[i].completed - samples[i - 1].completed;
    out.push(seconds > 0 && done >= 0 ? done / seconds : 0);
  }
  return out;
}

function sparkline(values) {
  const W = 120, H = 24;
  const svg = document.createElementNS(SVG_NS, "svg");
  svg.setAttribute("viewBox", "0 0 " + W + " " + H);
  svg.setAttribute("width", W);
  svg.setAttribute("height", H);
  svg.setAttribute("aria-hidden", "true");
  if (values.length < 2) return svg;
  const max = Math.max(...values, 0.001);
  const step = W / (HISTORY - 2);
  const points = values
    .map((v, i) => (i * step).toFixed(1) + "," + (H - 1 - (v / max) * (H - 2)).toFixed(1))
    .join(" ");
  const line = document.createElementNS(SVG_NS, "polyline");
  line.setAttribute("points", points);
  line.setAttribute("class", "spark");
  svg.appendChild(line);
  return svg;
}

function countCell(n, flagClass) {
  const td = el("td");
  td.appendChild(n > 0 && flagClass ? el("span", n, flagClass) : document.createTextNode(String(n)));
  return td;
}

function renderQueues(queues) {
  const card = document.getElementById("queues");
  card.replaceChildren();
  if (queues.length === 0) {
    card.appendChild(el("div", "No jobs yet. Enqueue one and it will show up here.", "empty"));
    return;
  }
  const table = el("table");
  const head = el("tr");
  for (const h of ["Queue", "Ready", "Scheduled", "Running", "Stalled", "Completed", "Dead", "Oldest wait", "Throughput"]) {
    head.appendChild(el("th", h));
  }
  table.appendChild(el("thead")).appendChild(head);
  const body = table.appendChild(el("tbody"));
  for (const q of queues) {
    const row = el("tr");
    const name = el("button", q.queue, "queue");
    name.type = "button";
    name.setAttribute("aria-pressed", String(q.queue === selected));
    name.addEventListener("click", () => {
      selected = q.queue;
      refresh();
    });
    row.appendChild(el("td")).appendChild(name);
    row.appendChild(countCell(q.ready));
    row.appendChild(countCell(q.scheduled));
    row.appendChild(countCell(q.running));
    row.appendChild(countCell(q.stalled, "warn"));
    row.appendChild(countCell(q.completed));
    row.appendChild(countCell(q.dead, "bad"));
    row.appendChild(el("td", formatAge(q.oldestReadyAgeMs)));
    const r = rates(history.get(q.queue) || []);
    const rate = el("span", null, "rate");
    rate.appendChild(sparkline(r));
    rate.appendChild(el("span", r.length ? r[r.length - 1].toFixed(1) + "/s" : "\\u2014"));
    row.appendChild(el("td")).appendChild(rate);
    body.appendChild(row);
  }
  card.appendChild(table);
}

async function renderDead(queue) {
  const title = document.getElementById("dead-title");
  const card = document.getElementById("dead");
  if (!queue) {
    title.textContent = "Dead-letter queue";
    card.replaceChildren(el("div", "Select a queue above.", "empty"));
    return;
  }
  title.textContent = "Dead-letter queue: " + queue;
  const res = await fetch("api/dead?queue=" + encodeURIComponent(queue), { cache: "no-store" });
  if (!res.ok) throw new Error("dead-letter request failed: " + res.status);
  const { jobs } = await res.json();
  card.replaceChildren();
  if (jobs.length === 0) {
    card.appendChild(el("div", "No dead jobs in this queue.", "empty"));
    return;
  }
  const table = el("table");
  const head = el("tr");
  for (const h of ["Job", "Attempts", "Last error", "Died at"]) head.appendChild(el("th", h));
  table.appendChild(el("thead")).appendChild(head);
  const body = table.appendChild(el("tbody"));
  for (const job of jobs) {
    const row = el("tr");
    row.appendChild(el("td")).appendChild(el("code", job.id));
    row.appendChild(el("td", job.attempts + " / " + job.maxAttempts));
    row.appendChild(el("td", job.error, "wrap"));
    row.appendChild(el("td", job.finishedAt === null ? "\\u2014" : new Date(job.finishedAt).toLocaleTimeString()));
    body.appendChild(row);
  }
  card.appendChild(table);
}

let lastQueues = [];

function record(queues) {
  const seen = new Set();
  for (const q of queues) {
    seen.add(q.queue);
    const samples = history.get(q.queue) || [];
    samples.push({ at: q.at, completed: q.completed });
    if (samples.length > HISTORY) samples.shift();
    history.set(q.queue, samples);
  }
  for (const name of history.keys()) if (!seen.has(name)) history.delete(name);
}

async function refresh() {
  const status = document.getElementById("status");
  try {
    renderQueues(lastQueues);
    await renderDead(selected);
    status.textContent = "Updated " + new Date().toLocaleTimeString();
    status.className = "";
  } catch (err) {
    status.textContent = "Can't reach the server; retrying\\u2026";
    status.className = "error";
  }
}

async function poll() {
  try {
    const res = await fetch("api/stats", { cache: "no-store" });
    if (!res.ok) throw new Error("stats request failed: " + res.status);
    const { queues } = await res.json();
    record(queues);
    lastQueues = queues;
    if (selected === null || !queues.some((q) => q.queue === selected)) {
      const withDead = queues.find((q) => q.dead > 0);
      selected = withDead ? withDead.queue : queues.length ? queues[0].queue : null;
    }
    await refresh();
  } catch (err) {
    const status = document.getElementById("status");
    status.textContent = "Can't reach the server; retrying\\u2026";
    status.className = "error";
  } finally {
    // setTimeout after each poll, not setInterval: a slow response can't stack up requests.
    setTimeout(poll, POLL_MS);
  }
}

poll();
`;

export function renderPage(): string {
  return `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Job queue dashboard</title>
<style>${PAGE_CSS}</style>
</head>
<body>
<main>
  <header>
    <h1>Job queue dashboard</h1>
    <span id="status" role="status">Loading…</span>
  </header>
  <div class="card" id="queues"></div>
  <p class="legend">
    <strong>Ready</strong>: waiting for a free worker. <strong>Scheduled</strong>: delayed, or a retry waiting out its backoff.
    <strong>Stalled</strong>: running, but the worker's lease lapsed; the next claim takes it over.
    <strong>Oldest wait</strong>: how long the oldest ready job has been waiting.
  </p>
  <h2 id="dead-title">Dead-letter queue</h2>
  <div class="card" id="dead"></div>
</main>
<script>${PAGE_JS}</script>
</body>
</html>
`;
}
