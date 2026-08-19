"""Report PumpPortal versus Jupiter feed coverage, timing, rate, and reliability."""

from __future__ import annotations

import sqlite3
import statistics
from datetime import UTC, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DATABASE_PATH = REPO_ROOT / "data" / "feed_benchmark.db"
REPORT_PATH = REPO_ROOT / "data" / "FEED_BENCHMARK_REPORT.md"
REPLAY_BASELINE_PER_MINUTE = 31.0


def parse_timestamp(value: str) -> datetime:
    """Parse the stored UTC timestamp format."""
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def percentile(values: list[float], fraction: float) -> float:
    """Return a linear-interpolated percentile without a NumPy dependency."""
    if not values:
        return 0.0
    ordered = sorted(values)
    index = (len(ordered) - 1) * fraction
    lower = int(index)
    upper = min(lower + 1, len(ordered) - 1)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (index - lower)


def format_seconds(value: float) -> str:
    return f"{value:.3f}s"


def latest_run(connection: sqlite3.Connection) -> tuple[int, str, str | None] | None:
    return connection.execute(
        "SELECT id, started_at, stopped_at FROM benchmark_runs ORDER BY id DESC LIMIT 1"
    ).fetchone()


def first_detections(connection: sqlite3.Connection, run_id: int) -> dict[str, dict[str, str]]:
    rows = connection.execute(
        """
        SELECT source, mint, MIN(detected_at) AS first_detected_at
        FROM feed_events
        WHERE run_id = ? AND source IN ('pumpportal', 'jupiter')
        GROUP BY source, mint
        """,
        (run_id,),
    ).fetchall()
    results: dict[str, dict[str, str]] = {"pumpportal": {}, "jupiter": {}}
    for source, mint, detected_at in rows:
        results[source][mint] = detected_at
    return results


def gap_seconds(timestamps: list[str], start: str, end: str) -> float:
    """Find the longest PumpPortal silence, including beginning/end of the run."""
    points = [parse_timestamp(start)]
    points.extend(parse_timestamp(value) for value in timestamps)
    points.append(parse_timestamp(end))
    if len(points) < 2:
        return 0.0
    return max(
        (later - earlier).total_seconds()
        for earlier, later in zip(points, points[1:], strict=False)
    )


def disconnect_durations(connection: sqlite3.Connection, run_id: int, ended_at: str) -> list[float]:
    rows = connection.execute(
        """
        SELECT event, occurred_at FROM feed_connection_events
        WHERE run_id = ? ORDER BY occurred_at
        """,
        (run_id,),
    ).fetchall()
    durations: list[float] = []
    disconnected_at: str | None = None
    for event, occurred_at in rows:
        if event == "disconnected":
            disconnected_at = occurred_at
        elif event == "connected" and disconnected_at is not None:
            duration = parse_timestamp(occurred_at) - parse_timestamp(disconnected_at)
            durations.append(duration.total_seconds())
            disconnected_at = None
    if disconnected_at is not None:
        duration = parse_timestamp(ended_at) - parse_timestamp(disconnected_at)
        durations.append(duration.total_seconds())
    return durations


def build_report(connection: sqlite3.Connection) -> str:
    run = latest_run(connection)
    if run is None:
        return "# Feed Benchmark Report\n\nNo benchmark runs have been recorded yet.\n"

    run_id, started_at, stopped_at = run
    ended_at = stopped_at or datetime.now(UTC).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )
    detections = first_detections(connection, run_id)
    pump_mints = set(detections["pumpportal"])
    jupiter_mints = set(detections["jupiter"])
    both = pump_mints & jupiter_mints
    pump_only = pump_mints - jupiter_mints
    jupiter_only = jupiter_mints - pump_mints
    union_count = len(pump_mints | jupiter_mints)
    overlap_percentage = (len(both) / union_count * 100) if union_count else 0.0

    deltas = []
    for mint in both:
        jupiter_at = parse_timestamp(detections["jupiter"][mint])
        pumpportal_at = parse_timestamp(detections["pumpportal"][mint])
        deltas.append((jupiter_at - pumpportal_at).total_seconds())
    pump_first = [delta for delta in deltas if delta > 0]
    jupiter_first = [-delta for delta in deltas if delta < 0]
    simultaneous = sum(delta == 0 for delta in deltas)
    pump_lead = format_seconds(statistics.median(pump_first)) if pump_first else "n/a"
    jupiter_lead = format_seconds(statistics.median(jupiter_first)) if jupiter_first else "n/a"
    absolute_deltas = [abs(delta) for delta in deltas]
    histogram = {
        "<1s": sum(value < 1 for value in absolute_deltas),
        "1-5s": sum(1 <= value < 5 for value in absolute_deltas),
        "5-10s": sum(5 <= value < 10 for value in absolute_deltas),
        "10-30s": sum(10 <= value < 30 for value in absolute_deltas),
        "30s+": sum(value >= 30 for value in absolute_deltas),
    }

    event_counts = dict(
        connection.execute(
            "SELECT source, COUNT(*) FROM feed_events "
            "WHERE run_id = ? AND source IN ('pumpportal', 'jupiter') GROUP BY source",
            (run_id,),
        ).fetchall()
    )
    elapsed_seconds = (parse_timestamp(ended_at) - parse_timestamp(started_at)).total_seconds()
    elapsed_minutes = max(elapsed_seconds / 60, 0.001)
    pump_rate = len(pump_mints) / elapsed_minutes
    jupiter_rate = len(jupiter_mints) / elapsed_minutes
    pump_timestamps = [
        row[0]
        for row in connection.execute(
            "SELECT detected_at FROM feed_events "
            "WHERE run_id = ? AND source = 'pumpportal' ORDER BY detected_at",
            (run_id,),
        )
    ]
    disconnects = disconnect_durations(connection, run_id, ended_at)
    longest_gap = gap_seconds(pump_timestamps, started_at, ended_at)

    lines = [
        "# PumpPortal vs Jupiter Feed Benchmark",
        "",
        f"- Run ID: `{run_id}`",
        f"- UTC start: `{started_at}`",
        f"- UTC end: `{ended_at}`{' (still running)' if stopped_at is None else ''}",
        f"- Observed duration: `{elapsed_minutes:.2f}` minutes",
        "",
        "## Coverage Comparison",
        "",
        "| Metric | Mints |",
        "| --- | ---: |",
        f"| PumpPortal only | {len(pump_only):,} |",
        f"| Jupiter only | {len(jupiter_only):,} |",
        f"| Both feeds | {len(both):,} |",
        f"| Union | {union_count:,} |",
        f"| Overlap (both / union) | {overlap_percentage:.2f}% |",
        "",
        "## Timing Comparison",
        "",
        "`delta = Jupiter first detection - PumpPortal first detection`; a positive value "
        "means PumpPortal was first.",
        "",
        "| Statistic | Detection delta |",
        "| --- | ---: |",
        f"| Median | {format_seconds(statistics.median(deltas)) if deltas else 'n/a'} |",
        f"| P25 | {format_seconds(percentile(deltas, 0.25)) if deltas else 'n/a'} |",
        f"| P75 | {format_seconds(percentile(deltas, 0.75)) if deltas else 'n/a'} |",
        f"| P95 | {format_seconds(percentile(deltas, 0.95)) if deltas else 'n/a'} |",
        f"| Mean | {format_seconds(statistics.mean(deltas)) if deltas else 'n/a'} |",
        "",
        f"- PumpPortal first: `{len(pump_first):,}` mints; median lead `{pump_lead}`.",
        f"- Jupiter first: `{len(jupiter_first):,}` mints; median lead `{jupiter_lead}`.",
        f"- Same millisecond: `{simultaneous:,}` mints.",
        "",
        "| Absolute detection gap bucket | Mints |",
        "| --- | ---: |",
        *(f"| {bucket} | {count:,} |" for bucket, count in histogram.items()),
        "",
        "## Rate Comparison",
        "",
        "Rates use unique mints divided by the full benchmark duration.",
        "",
        "| Source | Unique mints | Events logged | Mints/min | Difference vs replay 31/min |",
        "| --- | ---: | ---: | ---: | ---: |",
        f"| PumpPortal | {len(pump_mints):,} | {event_counts.get('pumpportal', 0):,} | "
        f"{pump_rate:.2f} | {pump_rate - REPLAY_BASELINE_PER_MINUTE:+.2f} |",
        f"| Jupiter | {len(jupiter_mints):,} | {event_counts.get('jupiter', 0):,} | "
        f"{jupiter_rate:.2f} | {jupiter_rate - REPLAY_BASELINE_PER_MINUTE:+.2f} |",
        "",
        "## PumpPortal Reliability",
        "",
        f"- Disconnects observed: `{len(disconnects)}`.",
        "- Reconnect/outage durations: `"
        f"{', '.join(format_seconds(value) for value in disconnects) if disconnects else 'none'}`.",
        f"- Longest period with zero PumpPortal token events: `{format_seconds(longest_gap)}`.",
        "",
        "## Replay Comparison Handoff",
        "",
        f"Use the exact UTC window `{started_at}` through `{ended_at}` "
        "for the separate enriched-parquet replay comparison.",
        "",
    ]
    return "\n".join(lines)


def main() -> None:
    if not DATABASE_PATH.exists():
        raise SystemExit(f"Benchmark database does not exist: {DATABASE_PATH}")
    connection = sqlite3.connect(DATABASE_PATH)
    try:
        report = build_report(connection)
    finally:
        connection.close()
    REPORT_PATH.write_text(report)
    print(report)
    print(f"Saved report to {REPORT_PATH}")


if __name__ == "__main__":
    main()
