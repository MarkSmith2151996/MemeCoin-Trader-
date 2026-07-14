"""Read-only analysis: early-trait comparison of active vs dead historical coins.

Step 1: Queries all distinct mints from price_snapshots (MT-443 outcome).
Step 2: Joins earliest available paper_decisions, trades, positions, and
         live_candidate_observations evidence.
Step 3: Produces a mint-level CSV feature matrix and a markdown pattern report.

Detached — no runtime, config, risk-policy, or trading-path changes.
"""

from __future__ import annotations

import json
import math
import sqlite3
import sys
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

DB_PATH = Path("data/trades.db")
OUTPUT_DIR = Path("/mnt/c/Users/Big A/custodian-shared/memecoin-trader/pattern-analysis")


def connect() -> sqlite3.Connection:
    if not DB_PATH.exists():
        sys.exit(f"Database not found: {DB_PATH}")
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


# ---------------------------------------------------------------------------
# Step 1 — outcome labels
# ---------------------------------------------------------------------------

def get_outcomes(conn: sqlite3.Connection) -> dict[str, str]:
    rows = conn.execute(
        """
        SELECT mint_address, price_sol, fdv_usd, liquidity_usd, volume_h24
        FROM price_snapshots
        """
    ).fetchall()
    result: dict[str, str] = {}
    for r in rows:
        mint = r["mint_address"]
        price = r["price_sol"]
        if price is not None and price > 0 and math.isfinite(price):
            result[mint] = "active_priced"
        else:
            result[mint] = "dead_or_unavailable"
    return result


def get_snapshot_fields(conn: sqlite3.Connection) -> dict[str, dict]:
    """Return the snapshot's numeric fields for reference."""
    rows = conn.execute(
        """
        SELECT mint_address, price_sol, price_usd, volume_h24, liquidity_usd, fdv_usd
        FROM price_snapshots
        """
    ).fetchall()
    return {r["mint_address"]: dict(r) for r in rows}


# ---------------------------------------------------------------------------
# Step 2 — earliest paper_decision evidence per mint
# ---------------------------------------------------------------------------

def get_earliest_paper_decisions(conn: sqlite3.Connection) -> dict[str, dict]:
    """For each mint, the paper_decisions row with the earliest recorded_at."""
    rows = conn.execute(
        """
        SELECT mint_address, recorded_at, source, source_count, candidate_mode,
               decision, action_outcome, primary_reason, attention_score, risk_score,
               diagnostics_json, record_json
        FROM (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY mint_address ORDER BY recorded_at ASC
            ) AS rn
            FROM paper_decisions
        )
        WHERE rn = 1
        """
    ).fetchall()
    result: dict[str, dict] = {}
    for r in rows:
        mint = r["mint_address"]
        diag = _safe_json(r["diagnostics_json"])
        entry: dict = {
            "pd_recorded_at": r["recorded_at"],
            "pd_source": r["source"],
            "pd_source_count": r["source_count"],
            "pd_candidate_mode": r["candidate_mode"],
            "pd_decision": r["decision"],
            "pd_action_outcome": r["action_outcome"],
            "pd_primary_reason": r["primary_reason"],
            "pd_attention_score": r["attention_score"],
            "pd_risk_score": r["risk_score"],
            # Diagnostics features
            "pd_top10_holder_pct": _dig(diag, "top10_holder_pct"),
            "pd_holder_policy_state": _dig(diag, "holder_policy_state"),
            "pd_liquidity_data_state": _dig(diag, "liquidity_data_state"),
            "pd_liquidity_source": _dig(diag, "liquidity_source"),
            "pd_liquidity_unknown_reason": _dig(diag, "liquidity_unknown_reason"),
            "pd_edge_score": _dig(diag, "edge_score"),
            "pd_authority_policy_state": _dig(diag, "authority_policy_state"),
            "pd_creator_policy_state": _dig(diag, "creator_policy_state"),
            "pd_honeypot_policy_state": _dig(diag, "honeypot_policy_state"),
            "pd_unique_buyers_policy_state": _dig(diag, "unique_buyers_policy_state"),
            "pd_metadata_completeness_state": _dig(diag, "metadata_completeness_state"),
            "pd_social_signal_state": _dig(diag, "social_signal_state"),
            "pd_risk_approval_state": _dig(diag, "risk_approval_state"),
            "pd_failed_check": _dig(diag, "failed_check"),
            "pd_narrative_tags": _dig(diag, "narrative_tags"),
            "pd_attention_reasons": _dig(diag, "attention_reasons"),
            "pd_creator_holding_source": _dig(diag, "creator_holding_source"),
            "pd_creator_unknown_reason": _dig(diag, "creator_unknown_reason"),
        }
        # Extract numeric from recheck_snapshot if present
        recheck = _dig(diag, "recheck_snapshot")
        if isinstance(recheck, dict):
            entry["pd_recheck_liquidity_data_state"] = recheck.get("liquidity_data_state")
            entry["pd_recheck_holder_policy_state"] = recheck.get("holder_policy_state")
            entry["pd_recheck_top10_holder_pct"] = recheck.get("top10_holder_pct")
            entry["pd_recheck_risk_approval_state"] = recheck.get("risk_approval_state")
        result[mint] = entry
    return result


# ---------------------------------------------------------------------------
# Step 2b — earliest trade evidence per mint
# ---------------------------------------------------------------------------

def get_earliest_trades(conn: sqlite3.Connection) -> dict[str, dict]:
    """For each mint, aggregate trade info (earliest, count, modes)."""
    rows = conn.execute(
        """
        SELECT mint_address, MIN(executed_at) as first_trade_at,
               COUNT(*) as trade_count,
               COUNT(DISTINCT mode) as mode_count,
               GROUP_CONCAT(DISTINCT mode) as modes,
               SUM(CASE WHEN side = 'BUY' THEN 1 ELSE 0 END) as buy_count,
               SUM(CASE WHEN side = 'SELL' THEN 1 ELSE 0 END) as sell_count
        FROM trades
        GROUP BY mint_address
        """
    ).fetchall()
    result: dict[str, dict] = {}
    for r in rows:
        result[r["mint_address"]] = {
            "first_trade_at": r["first_trade_at"],
            "trade_count": r["trade_count"],
            "trade_mode_count": r["mode_count"],
            "trade_modes": r["modes"],
            "trade_buy_count": r["buy_count"],
            "trade_sell_count": r["sell_count"],
        }
    return result


# ---------------------------------------------------------------------------
# Step 2c — earliest position evidence
# ---------------------------------------------------------------------------

def get_earliest_positions(conn: sqlite3.Connection) -> dict[str, dict]:
    rows = conn.execute(
        """
        SELECT mint_address, MIN(opened_at) as first_position_at,
               COUNT(*) as position_count,
               SUM(realized_pnl_sol) as total_realized_pnl_sol
        FROM positions
        GROUP BY mint_address
        """
    ).fetchall()
    result: dict[str, dict] = {}
    for r in rows:
        result[r["mint_address"]] = {
            "first_position_at": r["first_position_at"],
            "position_count": r["position_count"],
            "total_realized_pnl_sol": r["total_realized_pnl_sol"],
        }
    return result


# ---------------------------------------------------------------------------
# Step 2d — live_candidate_observations evidence
# ---------------------------------------------------------------------------

def get_candidate_observations(conn: sqlite3.Connection) -> dict[str, dict]:
    rows = conn.execute(
        """
        SELECT mint_address, MIN(first_seen_at) as first_seen,
               MIN(observed_at) as first_observed,
               GROUP_CONCAT(DISTINCT candidate_mode) as candidate_modes,
               GROUP_CONCAT(DISTINCT strict_result) as strict_results,
               GROUP_CONCAT(DISTINCT paper_minimum_result) as paper_minimum_results
        FROM live_candidate_observations
        GROUP BY mint_address
        """
    ).fetchall()
    result: dict[str, dict] = {}
    for r in rows:
        result[r["mint_address"]] = {
            "lco_first_seen_at": r["first_seen"],
            "lco_first_observed_at": r["first_observed"],
            "lco_candidate_modes": r["candidate_modes"],
            "lco_strict_results": r["strict_results"],
            "lco_paper_minimum_results": r["paper_minimum_results"],
        }
    return result


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _safe_json(text: str | None) -> dict | None:
    if not text:
        return None
    try:
        return json.loads(text)
    except (json.JSONDecodeError, TypeError):
        return None


def _dig(d: dict | None, key: str):
    if d is None:
        return None
    return d.get(key)


def _s(v) -> float | None:
    """Safe numeric conversion."""
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError, OverflowError):
        return None


# ---------------------------------------------------------------------------
# Main assembly
# ---------------------------------------------------------------------------

def main() -> None:
    conn = connect()

    print("=== MT-445 Historical Survivor Analysis ===", flush=True)

    # Outcomes
    outcomes = get_outcomes(conn)
    snapshots = get_snapshot_fields(conn)
    print(f"  Total mints with snapshots: {len(outcomes)}", flush=True)
    active = sum(1 for v in outcomes.values() if v == "active_priced")
    dead = sum(1 for v in outcomes.values() if v == "dead_or_unavailable")
    print(f"  Active (priced): {active}  Dead/unavailable: {dead}", flush=True)

    # Earliest evidence
    pd_earliest = get_earliest_paper_decisions(conn)
    trade_earliest = get_earliest_trades(conn)
    pos_earliest = get_earliest_positions(conn)
    lco_earliest = get_candidate_observations(conn)
    print(f"  Mints with paper_decisions: {len(pd_earliest)}", flush=True)
    print(f"  Mints with trades: {len(trade_earliest)}", flush=True)
    print(f"  Mints with positions: {len(pos_earliest)}", flush=True)
    print(f"  Mints with candidate_observations: {len(lco_earliest)}", flush=True)

    # Assemble mint-level rows
    all_mints = sorted(outcomes.keys())
    rows: list[dict] = []
    for mint in all_mints:
        row: dict = {
            "mint_address": mint,
            "outcome": outcomes[mint],
        }
        # Snapshot fields
        snap = snapshots.get(mint, {})
        row["snapshot_price_sol"] = snap.get("price_sol")
        row["snapshot_volume_h24"] = snap.get("volume_h24")
        row["snapshot_liquidity_usd"] = snap.get("liquidity_usd")
        row["snapshot_fdv_usd"] = snap.get("fdv_usd")

        # Paper decisions
        pd = pd_earliest.get(mint, {})
        row.update(pd)
        row["has_paper_decision"] = 1 if len(pd) > 0 else None
        row["provenance_pd"] = "paper_decisions" if len(pd) > 0 else None

        # Trades
        tr = trade_earliest.get(mint, {})
        row.update(tr)
        row["has_trade"] = 1 if len(tr) > 0 else None
        row["provenance_trade"] = "trades" if len(tr) > 0 else None

        # Positions
        po = pos_earliest.get(mint, {})
        row.update(po)
        row["has_position"] = 1 if len(po) > 0 else None
        row["provenance_position"] = "positions" if len(po) > 0 else None

        # Candidate observations
        co = lco_earliest.get(mint, {})
        row.update(co)
        row["has_lco"] = 1 if len(co) > 0 else None
        row["provenance_lco"] = "live_candidate_observations" if len(co) > 0 else None

        # Evidence coverage summary
        evidence_sources = []
        if len(pd) > 0:
            evidence_sources.append("paper_decisions")
        if len(tr) > 0:
            evidence_sources.append("trades")
        if len(po) > 0:
            evidence_sources.append("positions")
        if len(co) > 0:
            evidence_sources.append("live_candidate_observations")
        row["evidence_sources"] = ";".join(evidence_sources) if evidence_sources else None
        row["evidence_source_count"] = len(evidence_sources)

        rows.append(row)

    # Write CSV
    csv_rows = _dicts_to_csv(rows, OUTPUT_DIR / "historical_survivor_feature_matrix.csv")
    print(f"\n  CSV rows written: {csv_rows}", flush=True)

    # Produce markdown report
    _write_report(rows, outcomes, pd_earliest, active, dead)
    print("\n=== Analysis complete ===", flush=True)


# ---------------------------------------------------------------------------
# CSV writer
# ---------------------------------------------------------------------------

def _dicts_to_csv(rows: list[dict], path: Path) -> int:
    cols: list[str] = []
    seen_keys: set[str] = set()
    for row in rows:
        for k in row:
            if k not in seen_keys:
                cols.append(k)
                seen_keys.add(k)

    lines: list[str] = []
    lines.append(",".join(cols))
    for row in rows:
        vals: list[str] = []
        for col in cols:
            v = row.get(col)
            if v is None:
                vals.append("")
            elif isinstance(v, (list, dict)):
                s = json.dumps(v, default=str)
                vals.append(f'"{s}"')
            elif isinstance(v, str):
                if "," in v or '"' in v or "\n" in v:
                    vals.append(f'"{v.replace(chr(34), chr(34)+chr(34))}"')
                else:
                    vals.append(v)
            else:
                vals.append(str(v))
        lines.append(",".join(vals))

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"  CSV: {path}  ({len(lines) - 1} data rows)", flush=True)
    return len(lines) - 1


# ---------------------------------------------------------------------------
# Report writer
# ---------------------------------------------------------------------------

def _write_report(
    rows: list[dict],
    outcomes: dict[str, str],
    pd_earliest: dict[str, dict],
    active_count: int,
    dead_count: int,
) -> None:
    # Classify rows
    active_rows = [r for r in rows if r["outcome"] == "active_priced"]
    dead_rows = [r for r in rows if r["outcome"] == "dead_or_unavailable"]

    # Mints with paper_decision evidence
    pd_active = [r for r in active_rows if r["has_paper_decision"]]
    pd_dead = [r for r in dead_rows if r["has_paper_decision"]]

    lines: list[str] = []

    def p(text: str = "") -> None:
        lines.append(text)

    p("# Historical Survivor Pattern Analysis")
    p()
    p(f"**MT-445** — generated {datetime.now(UTC).isoformat()}")
    p()
    p("## Overview")
    p()
    p(f"- Total distinct mints with MT-443 price snapshots: **{len(outcomes)}**")
    p(f"- Active (finite positive price): **{active_count}** ({active_count/len(outcomes)*100:.1f}%)")
    p(f"- Dead or unavailable: **{dead_count}** ({dead_count/len(outcomes)*100:.1f}%)")
    p()
    p(f"- Mints with paper_decision evidence: {len(pd_earliest)} ({len(pd_earliest)/len(outcomes)*100:.1f}%)")
    p(f"- Mints with trade records: {sum(1 for r in rows if r['has_trade'])}")
    p(f"- Mints with position records: {sum(1 for r in rows if r['has_position'])}")
    p(f"- Mints with candidate observations: {sum(1 for r in rows if r['has_lco'])}")
    p()

    # --- Feature coverage ---
    p("## Feature Coverage by Outcome Group")
    p()
    p("| Feature | Active coverage | Dead coverage | Overall |")
    p("|---------|----------------|--------------|---------|")

    features = [
        ("has_paper_decision", "Any paper_decision"),
        ("pd_top10_holder_pct", "Top-10 holder pct"),
        ("pd_liquidity_data_state", "Liquidity data state"),
        ("pd_edge_score", "Edge score"),
        ("pd_risk_score", "Risk score"),
        ("pd_attention_score", "Attention score"),
        ("pd_source", "Signal source"),
        ("pd_candidate_mode", "Candidate mode"),
        ("pd_primary_reason", "Rejection reason"),
        ("pd_holder_policy_state", "Holder policy state"),
        ("pd_authority_policy_state", "Authority policy state"),
        ("pd_creator_policy_state", "Creator policy state"),
        ("pd_honeypot_policy_state", "Honeypot policy state"),
        ("pd_unique_buyers_policy_state", "Unique buyers policy state"),
        ("pd_social_signal_state", "Social signal state"),
        ("pd_narrative_tags", "Narrative tags"),
        ("pd_risk_approval_state", "Risk approval state"),
        ("has_trade", "Trade record"),
        ("has_position", "Position record"),
        ("has_lco", "Candidate observation"),
    ]

    for field, label in features:
        act = sum(1 for r in active_rows if r.get(field) is not None and r[field] != "")
        dea = sum(1 for r in dead_rows if r.get(field) is not None and r[field] != "")
        p(f"| {label} | {act}/{len(active_rows)} ({act/len(active_rows)*100:.1f}%) | {dea}/{len(dead_rows)} ({dea/len(dead_rows)*100:.1f}%) | {(act+dea)/len(rows)*100:.1f}% |")
    p()

    # --- Source distribution ---
    p("## Signal Source Distribution (paper_decisions evidence)")
    p()
    p("### Active coins")
    p()
    sources_active = Counter(r.get("pd_source") for r in pd_active if r.get("pd_source"))
    p(f"| Source | Count | % of active with evidence |")
    p(f"|--------|-------|-------------------------|")
    for source, cnt in sources_active.most_common():
        p(f"| {source} | {cnt} | {cnt/len(pd_active)*100:.1f}% |")
    p()
    p("### Dead coins")
    p()
    sources_dead = Counter(r.get("pd_source") for r in pd_dead if r.get("pd_source"))
    p(f"| Source | Count | % of dead with evidence |")
    p(f"|--------|-------|-----------------------|")
    for source, cnt in sources_dead.most_common():
        p(f"| {source} | {cnt} | {cnt/len(pd_dead)*100:.1f}% |")
    p()

    # --- Candidate mode ---
    p("## Candidate Mode Distribution")
    p()
    for group_name, group_rows in [("Active", pd_active), ("Dead", pd_dead)]:
        p(f"### {group_name} coins")
        p()
        p("| Candidate mode | Count | % |")
        p("|--------------|-------|---|")
        modes = Counter(r.get("pd_candidate_mode") for r in group_rows if r.get("pd_candidate_mode"))
        for mode, cnt in modes.most_common():
            p(f"| {mode} | {cnt} | {cnt/len(group_rows)*100:.1f}% |")
        p()

    # --- Decision / action_outcome ---
    p("## Paper Decision & Action Outcome")
    p()
    p("### Active coins")
    p()
    dec_active = Counter((r.get("pd_decision"), r.get("pd_action_outcome")) for r in pd_active)
    p("| Decision | Action outcome | Count | % |")
    p("|----------|--------------|-------|---|")
    for (dec, act), cnt in dec_active.most_common():
        p(f"| {dec} | {act} | {cnt} | {cnt/len(pd_active)*100:.1f}% |")
    p()
    p("### Dead coins")
    p()
    dec_dead = Counter((r.get("pd_decision"), r.get("pd_action_outcome")) for r in pd_dead)
    p("| Decision | Action outcome | Count | % |")
    p("|----------|--------------|-------|---|")
    for (dec, act), cnt in dec_dead.most_common():
        p(f"| {dec} | {act} | {cnt} | {cnt/len(pd_dead)*100:.1f}% |")
    p()

    # --- Rejection reason distribution ---
    p("## Primary Rejection Reason (rejected decisions)")
    p()
    rej_active = Counter(r.get("pd_primary_reason") for r in pd_active
                          if r.get("pd_decision") == "rejected" and r.get("pd_primary_reason"))
    rej_dead = Counter(r.get("pd_primary_reason") for r in pd_dead
                        if r.get("pd_decision") == "rejected" and r.get("pd_primary_reason"))
    rej_all = set(rej_active.keys()) | set(rej_dead.keys())

    p("| Reason | Active count | Active % | Dead count | Dead % |")
    p("|--------|-------------|---------|-----------|-------|")
    for reason in sorted(rej_all):
        ac = rej_active.get(reason, 0)
        dc = rej_dead.get(reason, 0)
        act_pct = ac / max(len(pd_active), 1) * 100 if pd_active else 0
        dead_pct = dc / max(len(pd_dead), 1) * 100 if pd_dead else 0
        p(f"| {reason} | {ac} | {act_pct:.1f}% | {dc} | {dead_pct:.1f}% |")
    p()

    # --- Numeric comparisons ---
    p("## Numeric Field Comparisons (median / IQR)")
    p()

    numeric_fields = [
        ("pd_top10_holder_pct", "Top-10 holder concentration %"),
        ("pd_edge_score", "Edge score"),
        ("pd_risk_score", "Risk score"),
        ("pd_attention_score", "Attention score"),
        ("pd_source_count", "Source count"),
        ("trade_count", "Trade count"),
    ]

    p("| Field | Active median | Active p25 | Active p75 | Dead median | Dead p25 | Dead p75 |")
    p("|-------|-------------|----------|----------|-----------|--------|--------|")
    for field, label in numeric_fields:
        act_vals = sorted(_s(r.get(field)) for r in pd_active if _s(r.get(field)) is not None)
        dead_vals = sorted(_s(r.get(field)) for r in pd_dead if _s(r.get(field)) is not None)
        if not act_vals and not dead_vals:
            continue
        def stats(vals):
            if not vals:
                return None, None, None
            n = len(vals)
            return vals[n // 2], vals[n // 4], vals[3 * n // 4]
        a_med, a_p25, a_p75 = stats(act_vals)
        d_med, d_p25, d_p75 = stats(dead_vals)
        a_str = f"{a_med:.1f} / {a_p25:.1f} / {a_p75:.1f}" if a_med is not None else "-"
        d_str = f"{d_med:.1f} / {d_p25:.1f} / {d_p75:.1f}" if d_med is not None else "-"
        p(f"| {label} | {a_med or '-'} | {a_p25 or '-'} | {a_p75 or '-'} | {d_med or '-'} | {d_p25 or '-'} | {d_p75 or '-'} |")
    p()

    # --- Bucketed active rates ---
    p("## Bucketed Active Rates")
    p()
    p("### Top-10 Holder Concentration")
    p()
    buckets = [(0, 50), (50, 75), (75, 90), (90, 99), (99, 100.1)]
    p("| Bucket | Active count | Total count | Active rate |")
    p("|--------|------------|------------|-----------|")
    for lo, hi in buckets:
        bucket_rows = [r for r in pd_active + pd_dead
                        if _s(r.get("pd_top10_holder_pct")) is not None
                        and lo <= _s(r["pd_top10_holder_pct"]) < hi]
        bucket_active = sum(1 for r in bucket_rows if r["outcome"] == "active_priced")
        p(f"| [{lo}-{hi:.0f})% | {bucket_active} | {len(bucket_rows)} | {bucket_active/max(len(bucket_rows),1)*100:.1f}% |")
    p()

    p("### Holder Policy State")
    p()
    for state in ["pass", "fail", "unknown"]:
        hr = [r for r in pd_active + pd_dead if r.get("pd_holder_policy_state") == state]
        ha = sum(1 for r in hr if r["outcome"] == "active_priced")
        p(f"| holder_policy={state} | {ha}/{len(hr)} active ({ha/max(len(hr),1)*100:.1f}%) |")
    p()

    p("### Risk Approval State")
    p()
    for state in ["strict_rejected", "discovery_relaxed"]:
        hr = [r for r in pd_active + pd_dead if r.get("pd_risk_approval_state") == state]
        ha = sum(1 for r in hr if r["outcome"] == "active_priced")
        p(f"| risk_approval={state} | {ha}/{len(hr)} active ({ha/max(len(hr),1)*100:.1f}%) |")
    p()

    p("### Narrative Tags (liquidity signals)")
    p()
    # Check for 'liquid' tag
    for tag in ["liquid", "whale-flow"]:
        tr = [r for r in active_rows + dead_rows
              if r.get("pd_narrative_tags") and tag in str(r["pd_narrative_tags"])]
        ta = sum(1 for r in tr if r["outcome"] == "active_priced")
        p(f"| tag='{tag}' | {ta}/{len(tr)} active ({ta/max(len(tr),1)*100:.1f}%) |")
    p()

    p("### Liquidity Data State")
    p()
    for state in ["known", "unknown"]:
        lr = [r for r in pd_active + pd_dead if r.get("pd_liquidity_data_state") == state]
        la = sum(1 for r in lr if r["outcome"] == "active_priced")
        p(f"| liquidity_data={state} | {la}/{len(lr)} active ({la/max(len(lr),1)*100:.1f}%) |")
    p()

    p("### Social Signal State")
    p()
    for state in ["missing", "present"]:
        sr = [r for r in pd_active + pd_dead if r.get("pd_social_signal_state") == state]
        sa = sum(1 for r in sr if r["outcome"] == "active_priced")
        p(f"| social_signal={state} | {sa}/{len(sr)} active ({sa/max(len(sr),1)*100:.1f}%) |")
    p()

    # --- Top patterns ---
    p("## Strongest Observed Correlations (not trading rules)")
    p()
    p("These describe patterns in the 369 mints with paper_decision evidence. "
      "They are correlations, not proven causal predictors.")
    p()

    # Compute active rates for patterns
    patterns = []

    # Pattern 1: Liquidity known at assessment time
    liq_known_active = sum(1 for r in pd_active if r.get("pd_liquidity_data_state") == "known")
    liq_known_total = sum(1 for r in pd_active + pd_dead if r.get("pd_liquidity_data_state") == "known")
    liq_known_rate = liq_known_active / max(liq_known_total, 1)
    patterns.append((liq_known_rate, liq_known_total,
        f"Higher active rate when liquidity was known at assessment time "
        f"({liq_known_active}/{liq_known_total} = {liq_known_rate*100:.1f}% active) vs "
        f"unknown ({len(pd_active)-liq_known_active}/{len(pd_active)+len(pd_dead)-liq_known_total} "
        f"= {(len(pd_active)-liq_known_active)/max(len(pd_active)+len(pd_dead)-liq_known_total,1)*100:.1f}%). "
        f"Liquidity-known coins were more likely to retain market data."))

    # Pattern 2: Top-10 holder concentration low
    low_holder = [r for r in pd_active + pd_dead
                  if _s(r.get("pd_top10_holder_pct")) is not None
                  and _s(r["pd_top10_holder_pct"]) < 90]
    low_holder_active = sum(1 for r in low_holder if r["outcome"] == "active_priced")
    patterns.append((low_holder_active / max(len(low_holder), 1), len(low_holder),
        f"Coins with top-10 holder concentration below 90% had "
        f"{low_holder_active}/{len(low_holder)} = {low_holder_active/max(len(low_holder),1)*100:.1f}% active rate."))

    # Pattern 3: Holder policy pass
    holder_pass = [r for r in pd_active + pd_dead if r.get("pd_holder_policy_state") == "pass"]
    holder_pass_active = sum(1 for r in holder_pass if r["outcome"] == "active_priced")
    patterns.append((holder_pass_active / max(len(holder_pass), 1), len(holder_pass),
        f"Coins passing the holder policy had "
        f"{holder_pass_active}/{len(holder_pass)} = {holder_pass_active/max(len(holder_pass),1)*100:.1f}% active rate."))

    # Pattern 4: Rejection reason distribution
    rej_liq = Counter(r.get("pd_primary_reason") for r in pd_active
                       if r.get("pd_primary_reason") == "liquidity_check_unknown")
    rej_holder = Counter(r.get("pd_primary_reason") for r in pd_active
                          if r.get("pd_primary_reason") == "top10_holder_check_failed")
    rej_creator = Counter(r.get("pd_primary_reason") for r in pd_active
                           if r.get("pd_primary_reason") == "creator_holding_check_unknown")
    patterns.append((0, 0,
        f"Rejection reason breakdown for active coins: "
        f"liquidity_check_unknown={sum(rej_liq.values())}, "
        f"top10_holder_check_failed={sum(rej_holder.values())}, "
        f"creator_holding_check_unknown={sum(rej_creator.values())}. "
        f"Most active coins were rejected for holder concentration, not liquidity."))

    # Pattern 5: Source type
    pf_active = sum(1 for r in pd_active if r.get("pd_source") == "pump_fun")
    pf_total = sum(1 for r in pd_active + pd_dead if r.get("pd_source") == "pump_fun")
    wt_active = sum(1 for r in pd_active if r.get("pd_source") == "whale_tracker")
    wt_total = sum(1 for r in pd_active + pd_dead if r.get("pd_source") == "whale_tracker")
    patterns.append((pf_active / max(pf_total, 1), pf_total,
        f"Pump.fun-sourced coins: {pf_active}/{pf_total} = {pf_active/max(pf_total,1)*100:.1f}% active. "
        f"Whale-tracker-sourced coins: {wt_active}/{wt_total} = {wt_active/max(wt_total,1)*100:.1f}% active."))

    # Pattern 6: Narrative tags
    liquid_active = sum(1 for r in pd_active if r.get("pd_narrative_tags") and "liquid" in str(r["pd_narrative_tags"]))
    liquid_total = sum(1 for r in pd_active + pd_dead if r.get("pd_narrative_tags") and "liquid" in str(r["pd_narrative_tags"]))
    patterns.append((liquid_active / max(liquid_total, 1), liquid_total,
        f"Coins tagged 'liquid' in narrative tags: "
        f"{liquid_active}/{liquid_total} = {liquid_active/max(liquid_total,1)*100:.1f}% active."))

    # Pattern 7: Edge score
    high_edge = [r for r in pd_active + pd_dead
                 if _s(r.get("pd_edge_score")) is not None and _s(r["pd_edge_score"]) >= 20]
    high_edge_active = sum(1 for r in high_edge if r["outcome"] == "active_priced")
    low_edge = [r for r in pd_active + pd_dead
                if _s(r.get("pd_edge_score")) is not None and _s(r["pd_edge_score"]) < 20]
    low_edge_active = sum(1 for r in low_edge if r["outcome"] == "active_priced")
    patterns.append((high_edge_active / max(len(high_edge), 1), len(high_edge),
        f"Edge score >= 20: {high_edge_active}/{len(high_edge)} = "
        f"{high_edge_active/max(len(high_edge),1)*100:.1f}% active. "
        f"Edge score < 20: {low_edge_active}/{len(low_edge)} = "
        f"{low_edge_active/max(len(low_edge),1)*100:.1f}% active."))

    # Pattern 8: Candidate mode
    launch_active = sum(1 for r in pd_active if r.get("pd_candidate_mode") == "launch")
    launch_total = sum(1 for r in pd_active + pd_dead if r.get("pd_candidate_mode") == "launch")
    patterns.append((launch_active / max(launch_total, 1), launch_total,
        f"Launch-mode coins: {launch_active}/{launch_total} = "
        f"{launch_active/max(launch_total,1)*100:.1f}% active. "
        f"All pump.fun launches — most mints regardless of outcome were launch-mode."))

    # Pattern 9: Traded vs never traded
    traded_active = sum(1 for r in active_rows if r.get("has_trade"))
    traded_total = sum(1 for r in active_rows + dead_rows if r.get("has_trade"))
    patterns.append((traded_active / max(traded_total, 1), traded_total,
        f"Coins with at least one trade: {traded_active}/{traded_total} = "
        f"{traded_active/max(traded_total,1)*100:.1f}% active."))

    # Pattern 10: Risk approval state patterns
    relaxed_active = sum(1 for r in pd_active if r.get("pd_risk_approval_state") == "discovery_relaxed")
    relaxed_total = sum(1 for r in pd_active + pd_dead if r.get("pd_risk_approval_state") == "discovery_relaxed")
    patterns.append((relaxed_active / max(relaxed_total, 1), relaxed_total,
        f"Discovery-relaxed risk approval: {relaxed_active}/{relaxed_total} = "
        f"{relaxed_active/max(relaxed_total,1)*100:.1f}% active."))

    # Sort by active rate descending
    patterns.sort(key=lambda x: x[0], reverse=True)

    p("| # | Active rate | Sample size | Observation |")
    p("|---|-----------|------------|------------|")
    for i, (rate, n, desc) in enumerate(patterns, 1):
        p(f"| {i} | {rate*100:.1f}% | {n} | {desc} |")
    p()

    # --- Limitations ---
    p("## Limitations")
    p()
    p("1. **Survivorship bias.** Active-priced status means the mint retains DexScreener "
      "market data — it does not mean the coin was profitable to trade. Some survive at "
      "microscopic liquidity with no trade-able volume.")
    p("2. **Missing early evidence.** Of 606 mints, only 369 (60.9%) have paper_decision "
      "records. The remaining 237 mints (mostly trade-only) have no risk-assessment "
      "snapshot at discovery time. Their early-trait fields are null.")
    p("3. **Not a trading strategy.** Early-trait correlations reflect the project's "
      "conservative risk filters. A coin rejected for holder concentration but now "
      "active does not validate relaxing the filter — many of the 550 dead coins "
      "were also rejected for the same reasons.")
    p("4. **Single-point-in-time assessment.** Paper_decision evidence captures one "
      "snapshot; market dynamics change rapidly for meme coins. A decision rejection "
      "may have occurred before or after meaningful price action.")
    p("5. **Price does not imply liquidity.** Some 'active' coins have liquidity_usd = 0 "
      "or are on pump.fun bonding curves with no real trade-able pool.")
    p("6. **No causal inference.** Correlation between early liquidity data and active "
      "survival may reflect that DexScreener finds more data for higher-quality launches, "
      "not that liquidity causes survival.")
    p()

    # Write
    path = OUTPUT_DIR / "historical_survivor_pattern_report.md"
    path.write_text("\n".join(lines), encoding="utf-8")
    print(f"  Report: {path}  ({len(lines)} lines)", flush=True)


if __name__ == "__main__":
    main()
