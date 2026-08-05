"use strict";
const { execFileSync, spawnSync } = require("node:child_process");
const fs = require("node:fs");
const path = require("node:path");

const LOOP_CMD = {
  A: { pattern: "run_paper_loop.py", script: "scripts/run_paper_loop.py", log: "/home/dev/paper_loop.log" },
  B: { pattern: "run_strategy_b.py", script: "scripts/run_strategy_b.py", log: "/home/dev/strategy_b.log" },
};

const BROWSER_PC_HEALTH_URL = "http://localhost:8099/health";

/** Returns pid (number) if a process matching the pattern is alive, else null. */
function processAlive(pattern) {
  try {
    const out = execFileSync("pgrep", ["-f", pattern], { encoding: "utf8", timeout: 5000 });
    const pids = out
      .split("\n")
      .map((s) => s.trim())
      .filter((s) => /^\d+$/.test(s));
    return pids.length ? Number(pids[0]) : null;
  } catch {
    return null;
  }
}

/** Returns {ok, detail} for the browser-pc health endpoint. */
function browserPcHealth() {
  try {
    const out = execFileSync("curl", ["-s", "--connect-timeout", "2", "--max-time", "5", BROWSER_PC_HEALTH_URL], {
      encoding: "utf8",
      timeout: 8000,
    });
    const parsed = safeJson(out);
    return { ok: !!out && !!(parsed && parsed.status === "ok"), detail: out.trim().slice(0, 120) };
  } catch {
    return { ok: false, detail: "unreachable" };
  }
}

/** Overall health snapshot for both loops + browser-pc. */
function healthSnapshot() {
  const snap = {
    A: { alive: false, pid: null },
    B: { alive: false, pid: null },
    browserPc: { ok: false, detail: "unreachable" },
  };
  for (const k of ["A", "B"]) {
    const pid = processAlive(LOOP_CMD[k].pattern);
    snap[k].alive = pid != null;
    snap[k].pid = pid;
  }
  snap.browserPc = browserPcHealth();
  return snap;
}

/** Scan a log file for ERROR/CRITICAL/Traceback lines inside the window. */
function scanLogErrors(filePath, windowMs) {
  try {
    const size = fs.statSync(filePath).size;
    const fd = fs.openSync(filePath, "r");
    const chunk = Buffer.alloc(Math.min(size, 512 * 1024));
    fs.readSync(fd, chunk, 0, chunk.length, Math.max(0, size - chunk.length));
    fs.closeSync(fd);
    const lines = chunk.toString("utf8").split("\n");
    const cutoff = Date.now() - windowMs;
    const hits = [];
    for (const line of lines) {
      if (!/(ERROR|CRITICAL|Traceback|Exception)/.test(line)) continue;
      const t = parseLogTime(line);
      if (t == null || t >= cutoff) hits.push(line.trim());
    }
    return { count: hits.length, last: hits[hits.length - 1] || null };
  } catch {
    return { count: 0, last: null };
  }
}

/** Parse "YYYY-MM-DD HH:MM:SS" prefix (local ET time in these logs) to ms. */
function parseLogTime(line) {
  const m = line.match(/^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2}):(\d{2})/);
  if (!m) return null;
  const t = new Date(+m[1], +m[2] - 1, +m[3], +m[4], +m[5], +m[6]).getTime();
  return Number.isFinite(t) ? t : null;
}

function loadEnvKey(key) {
  try {
    const text = fs.readFileSync(path.join(__dirname, "..", ".env"), "utf8");
    const m = text.match(new RegExp(`^${key}=(.*)$`, "m"));
    if (!m) return null;
    return m[1].trim().replace(/^["']|["']$/g, "");
  } catch {
    return null;
  }
}

/**
 * Helius credit status.
 * Primary: /v0/usage endpoint (plans that expose it) -> {used, limit}.
 * Fallback: RPC getHealth probe -> {exhausted: true} when the key reports
 * "max usage reached", or {ok: true} when healthy but no usage data exists.
 */
async function heliusUsage() {
  const key = loadEnvKey("HELIUS_API_KEY");
  if (!key) return { used: null, limit: null, rateLimited: false, exhausted: false, ok: false, error: "no HELIUS_API_KEY in .env" };
  try {
    const res = await fetch(`https://api.helius.xyz/v0/usage?apiKey=${encodeURIComponent(key)}`, {
      signal: AbortSignal.timeout(8000),
    });
    if (res.ok) {
      const j = await res.json();
      if (j && j.creditsUsed != null) {
        return {
          used: j.creditsUsed,
          limit: j.maxCreditLimit,
          rateLimited: !!j.rateLimitExceeded,
          exhausted: false,
          ok: true,
          error: null,
        };
      }
    }
  } catch {
    /* fall through to RPC probe */
  }
  try {
    const res = await fetch("https://mainnet.helius-rpc.com/?api-key=" + encodeURIComponent(key), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "getHealth", params: [] }),
      signal: AbortSignal.timeout(8000),
    });
    const j = await res.json().catch(() => null);
    const msg = (j && j.error && j.error.message) || "";
    if (/max usage reached/i.test(msg)) {
      return { used: null, limit: null, rateLimited: true, exhausted: true, ok: false, error: "max usage reached" };
    }
    if (res.ok || !j || !j.error) {
      return { used: null, limit: null, rateLimited: false, exhausted: false, ok: true, error: null };
    }
    return { used: null, limit: null, rateLimited: false, exhausted: false, ok: false, error: msg || `HTTP ${res.status}` };
  } catch (e) {
    return { used: null, limit: null, rateLimited: false, exhausted: false, ok: false, error: e.message || String(e) };
  }
}

/** Kill a trading loop by strategy letter. Returns {ok, message}. */
function killLoop(strategy) {
  const cfg = LOOP_CMD[strategy];
  if (!cfg) return { ok: false, message: `unknown strategy ${strategy}` };
  const r = spawnSync("pkill", ["-f", cfg.pattern], { timeout: 8000 });
  if (r.error) return { ok: false, message: `pkill failed: ${r.error.message}` };
  return { ok: true, message: `kill signal sent (rc=${r.status})` };
}

/** Start a trading loop by strategy letter, matching watchdog behavior. */
function startLoop(strategy, projectRoot) {
  const cfg = LOOP_CMD[strategy];
  if (!cfg) return { ok: false, message: `unknown strategy ${strategy}` };
  const r = spawnSync("bash", ["-lc", `cd ${JSON.stringify(projectRoot)} && nohup python3 ${cfg.script} >> ${cfg.log} 2>&1 &`], {
    timeout: 8000,
  });
  if (r.error) return { ok: false, message: `start failed: ${r.error.message}` };
  return { ok: true, message: "launched" };
}

function safeJson(s) {
  try {
    return JSON.parse(s);
  } catch {
    return null;
  }
}

module.exports = {
  LOOP_CMD,
  processAlive,
  browserPcHealth,
  healthSnapshot,
  scanLogErrors,
  heliusUsage,
  killLoop,
  startLoop,
  loadEnvKey,
};
