"""MT-563: Discovery lag profiling report.

Reads the discovery_lag table in data/trades.db (read-only) and prints the
definitive latency analysis:

  - bucket distribution (the <5s .. 60s+ histogram)
  - by token source (pump / raydium / pumpswap / unknown)
  - by hour of day (does discovery lag vary with time?)
  - passed-gates vs rejected tokens (do gate winners have different lag?)
  - final recommendation: "Jupiter polling sufficient" or "WebSocket recommended"

Recommendation rule (constants below):
  - Requires >= MIN_SAMPLES (500) for a definitive verdict; below that it says
    "insufficient samples" and shows the numbers anyway.
  - "Jupiter polling sufficient" when median lag < MEDIAN_OK_S and p95 lag
    < P95_OK_S. Otherwise "WebSocket recommended".

Run manually when enough samples have accumulated (500+):
    python3 scripts/discovery_lag_report.py

Read-only: never writes to the DB and does not touch runtime code.
"""

from __future__ import annotations

import sqlite3
import statistics
from collections import defaultdict
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = REPO_ROOT / "data" / "trades.db"

MIN_SAMPLES = 500
MEDIAN_OK_S = 10.0
P95_OK_S = 30.0

BUCKETS = (
    ("<5s", 0.0, 5.0),
    ("5-10s", 5.0, 10.0),
    ("10-20s", 10.0, 20.0),
    ("20-30s", 20.0, 30.0),
    ("30-60s", 30.0, 60.0),
    ("60s+", 60.0, None),
)

SOURCES = ("pump", "raydium", "pumpswap", "unknown")


def load_samples() -> list[tuple[str, str, str, str, float, int]]:
    """Return (mint, token_source, created_at, detected_at, lag_seconds, passed) rows."""
    if not DB_PATH.exists():
        print(f"DB not found: {DB_PATH} — nothing to report.")
        return []
    con = sqlite3.connect(DB_PATH)
    try:
        con.row_factory = sqlite3.Row
        table_exists = con.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'discovery_lag'",
        ).fetchone()
        if table_exists is None:
            print("discovery_lag table is not initialized yet — restart Strategy B, then re-run.")
            return []
        rows = con.execute(
            """
            SELECT mint_address, token_source, created_at, detected_at,
                   lag_seconds, passed_gates, recorded_at
            FROM discovery_lag
            ORDER BY recorded_at, id
            """
        ).fetchall()
    finally:
        con.close()
    return [
        (
            r["mint_address"], r["token_source"], r["created_at"], r["detected_at"],
            float(r["lag_seconds"]), int(r["passed_gates"]),
        )
        for r in rows
    ]


def bucket_stats(values: list[float]) -> list[tuple[str, int, float]]:
    n = len(values)
    out = []
    for label, lo, hi in BUCKETS:
        count = sum(1 for v in values if v >= lo and (hi is None or v < hi))
        out.append((label, count, 100.0 * count / n if n else 0.0))
    return out


def summarize(values: list[float]) -> str:
    if not values:
        return "n=0"
    sorted_v = sorted(values)
    n = len(sorted_v)
    p95 = sorted_v[min(n - 1, int(0.95 * n) - 1)]
    return (
        f"n={n} min={sorted_v[0]:.1f}s avg={sum(sorted_v) / n:.1f}s "
        f"median={statistics.median(sorted_v):.1f}s p95={p95:.1f}s max={sorted_v[-1]:.1f}s"
    )


def render(samples: list[tuple[str, str, str, str, float, int]]) -> str:
    lines: list[str] = []
    n = len(samples)
    lines.append(f"# Discovery Lag Report ({n} samples)")
    lines.append("")
    if not samples:
        lines.append("No discovery_lag rows yet — run Strategy B for a while and re-run.")
        return "\n".join(lines)

    all_lags = [s[4] for s in samples]

    lines.append("## Bucket distribution (all samples)")
    lines.append("")
    lines.append("| bucket | count | share |")
    lines.append("|--------|------:|------:|")
    for label, count, pct in bucket_stats(all_lags):
        lines.append(f"| {label} | {count} | {pct:.1f}% |")
    lines.append("")
    lines.append(f"`{summarize(all_lags)}`")
    lines.append("")

    lines.append("## By token source")
    lines.append("")
    lines.append("| source | n | median | p95 | max |")
    lines.append("|--------|--:|-------:|----:|----:|")
    for source in SOURCES:
        vals = [s[4] for s in samples if s[1] == source]
        if not vals:
            lines.append(f"| {source} | 0 | — | — | — |")
            continue
        sorted_v = sorted(vals)
        p95 = sorted_v[min(len(sorted_v) - 1, int(0.95 * len(sorted_v)) - 1)]
        lines.append(
            f"| {source} | {len(vals)} | {statistics.median(sorted_v):.1f}s | "
            f"{p95:.1f}s | {sorted_v[-1]:.1f}s |"
        )
    lines.append("")

    lines.append("## By hour of day (UTC, by detected_at)")
    lines.append("")
    by_hour: dict[int, list[float]] = defaultdict(list)
    for _, _, _, detected_at, lag, _ in samples:
        try:
            hour = datetime.fromisoformat(detected_at.replace("Z", "+00:00")).hour
        except (ValueError, AttributeError, TypeError):
            continue
        by_hour[hour].append(lag)
    lines.append("| hour | n | median | p95 | max |")
    lines.append("|-----:|--:|-------:|----:|----:|")
    for hour in range(24):
        vals = by_hour.get(hour, [])
        if not vals:
            lines.append(f"| {hour:02d} | 0 | — | — | — |")
            continue
        sorted_v = sorted(vals)
        p95 = sorted_v[min(len(sorted_v) - 1, int(0.95 * len(sorted_v)) - 1)]
        lines.append(
            f"| {hour:02d} | {len(vals)} | {statistics.median(sorted_v):.1f}s | "
            f"{p95:.1f}s | {sorted_v[-1]:.1f}s |"
        )
    lines.append("")

    lines.append("## Passed gates vs rejected")
    lines.append("")
    passed = [s[4] for s in samples if s[5] == 1]
    rejected = [s[4] for s in samples if s[5] == 0]
    lines.append(f"- Passed gates ({len(passed)}): `{summarize(passed)}`")
    lines.append(f"- Rejected    ({len(rejected)}): `{summarize(rejected)}`")
    if passed and rejected:
        lines.append("")
        lines.append("Rejection means a token's first screening happened while it was still")
        lines.append("young — a FAIL sample is discovery that reached the gate layer quickly")
        lines.append("(low lag = fast discovery), while a PASS sample is a token that got")
        lines.append("far enough to be tradeable. If rejected lags are *higher* than passed")
        lines.append("lags, discovery is slow and early rejects are only caught late.")
    lines.append("")

    lines.append("## Verdict")
    lines.append("")
    if n < MIN_SAMPLES:
        lines.append(
            f"Insufficient samples for a definitive verdict: {n} < {MIN_SAMPLES}. "
            "Run again once 500+ rows accumulate."
        )
    else:
        sorted_v = sorted(all_lags)
        median = statistics.median(sorted_v)
        p95 = sorted_v[min(n - 1, int(0.95 * n) - 1)]
        if median < MEDIAN_OK_S and p95 < P95_OK_S:
            verdict = "Jupiter polling sufficient"
        else:
            verdict = "WebSocket recommended"
        lines.append(
            f"median={median:.1f}s, p95={p95:.1f}s vs thresholds "
            f"(median < {MEDIAN_OK_S:.0f}s, p95 < {P95_OK_S:.0f}s) → **{verdict}**"
        )
    return "\n".join(lines)


def main() -> None:
    samples = load_samples()
    print(render(samples))


if __name__ == "__main__":
    main()
