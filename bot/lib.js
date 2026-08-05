"use strict";
const fs = require("node:fs");

const ET_TZ = "America/New_York";

const ET_FMT = new Intl.DateTimeFormat("en-US", {
  timeZone: ET_TZ,
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
  hourCycle: "h23",
});

const SHORT_FMT = new Intl.DateTimeFormat("en-US", {
  timeZone: ET_TZ,
  hour: "2-digit",
  minute: "2-digit",
  hourCycle: "h23",
  timeZoneName: "short",
});

/** ET calendar parts of a UTC instant: {y, m (1-12), d, h, mi, s} */
function etParts(date) {
  const m = {};
  for (const p of ET_FMT.formatToParts(date)) m[p.type] = p.value;
  return {
    y: +m.year,
    m: +m.month,
    d: +m.day,
    h: +m.hour,
    mi: +m.minute,
    s: +m.second,
  };
}

/** Offset in ms such that utcInstant - offset = same clock time in ET. */
function etOffsetMs(utcInstant) {
  const p = etParts(new Date(utcInstant));
  const asUtc = Date.UTC(p.y, p.m - 1, p.d, p.h, p.mi, p.s);
  return asUtc - utcInstant;
}

/**
 * UTC epoch ms for midnight ET of today + dayOffset days.
 * DST-safe: the offset at any point of the day is the same as at that
 * day's midnight except during the 2-3am transition hour, which never
 * overlaps a 00:00 boundary.
 */
function etDayStartUtc(dayOffset = 0) {
  const now = Date.now();
  const p = etParts(new Date(now));
  const offset = etOffsetMs(now);
  return Date.UTC(p.y, p.m - 1, p.d + dayOffset, 0, 0, 0) - offset;
}

function msUntilNextEtTime(hour, minute) {
  const now = Date.now();
  let next = etDayStartUtc(1) + (hour * 60 + minute) * 60000;
  if (next - now < 5000) next += 86400000;
  return next - now;
}

function parseIso(s) {
  const t = Date.parse(s);
  return Number.isFinite(t) ? t : null;
}

/** Signed SOL amount, e.g. "+0.0123" / "-0.0499". */
function fmtSol(x) {
  if (!Number.isFinite(x)) return "n/a";
  const sign = x >= 0 ? "+" : "-";
  return `${sign}${Math.abs(x).toFixed(4)} SOL`;
}

/** Signed short SOL, e.g. "+0.05" / "-0.42". */
function fmtSolShort(x) {
  if (!Number.isFinite(x)) return "n/a";
  const sign = x >= 0 ? "+" : "-";
  return `${sign}${Math.abs(x).toFixed(2)} SOL`;
}

function fmtUsd(x) {
  if (!Number.isFinite(x)) return "n/a";
  return `$${x.toLocaleString("en-US", { maximumFractionDigits: 0 })}`;
}

function pct(x) {
  if (!Number.isFinite(x)) return "n/a";
  return `${Math.round(x * 100)}%`;
}

/** Short ET display like "16:49 EDT". */
function fmtEtTime(ms) {
  return SHORT_FMT.format(new Date(ms));
}

/** Short ET date display like "Aug 5". */
function fmtEtDate(ms) {
  return new Intl.DateTimeFormat("en-US", {
    timeZone: ET_TZ,
    month: "short",
    day: "numeric",
  }).format(new Date(ms));
}

/**
 * Extract UPPER_SNAKE constants from a Python script.
 * Returns a map of name -> value text. Values that open a bracket
 * (list/tuple/dict literals) are captured across lines until the
 * brackets close.
 */
function parseScriptConstants(filePath) {
  const out = {};
  let text;
  try {
    text = fs.readFileSync(filePath, "utf8");
  } catch {
    return out;
  }
  const lines = text.split("\n");
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    const m = line.match(/^([A-Z][A-Z0-9_]*)\s*=\s*(.*)$/);
    if (!m) continue;
    let value = m[2].trim();
    if (value.startsWith("#")) continue;
    const key = m[1];
    let depth = 0;
    for (const ch of value) {
      if (ch === "[" || ch === "(" || ch === "{") depth++;
      else if (ch === "]" || ch === ")" || ch === "}") depth--;
    }
    if (depth > 0) {
      const parts = [value];
      while (i + 1 < lines.length && depth > 0) {
        i++;
        const next = lines[i].trim();
        parts.push(next);
        for (const ch of next) {
          if (ch === "[" || ch === "(" || ch === "{") depth++;
          else if (ch === "]" || ch === ")" || ch === "}") depth--;
        }
      }
      value = parts.join(" ");
    } else {
      value = value.split(/\s+#/)[0].trim();
    }
    out[key] = value;
  }
  return out;
}

/** Parse a Python number literal (int, float, underscores). */
function pyNum(raw) {
  if (raw == null) return null;
  const clean = String(raw).replace(/_/g, "").trim();
  if (!/^[-+]?\d+(\.\d+)?$/.test(clean)) return null;
  return Number(clean);
}

/** Parse a Python string literal (single/double quoted). */
function pyStr(raw) {
  if (raw == null) return null;
  const m = String(raw).trim().match(/^["'](.+)["']$/);
  return m ? m[1] : null;
}

/**
 * Extract (age, warn, reject) tuples from a Python HOLDER_TIERS literal.
 * Reject values may be numeric literals or UPPER_CASE constants resolved
 * through the parsed constants map (e.g. MAX_TOP10_HOLDER_PCT).
 */
function parseHolderTiers(raw, consts = {}) {
  if (!raw) return [];
  const tiers = [];
  const re = /\(\s*(\d+)\s*,\s*([\d.]+)\s*,\s*([\d.A-Za-z_]+)\s*\)/g;
  let m;
  while ((m = re.exec(raw)) !== null) {
    let reject = pyNum(m[3]);
    if (reject == null && consts[m[3]] != null) reject = pyNum(consts[m[3]]);
    if (reject == null) continue;
    tiers.push({ age: +m[1], warn: +m[2], reject });
  }
  return tiers;
}

/** Reduce phone/jid to digits only. */
function digitsOnly(s) {
  return String(s || "").replace(/\D/g, "");
}

module.exports = {
  ET_TZ,
  etParts,
  etOffsetMs,
  etDayStartUtc,
  msUntilNextEtTime,
  parseIso,
  fmtSol,
  fmtSolShort,
  fmtUsd,
  pct,
  fmtEtTime,
  fmtEtDate,
  parseScriptConstants,
  pyNum,
  pyStr,
  parseHolderTiers,
  digitsOnly,
};
