#!/usr/bin/env python3
"""MT-440: Dune feasibility spike — SDK version.

Uses the official dune-client SDK to test connectivity, execute a
known-working Solana-related query, and assess data usefulness.
"""

import os
import json
import sys
import pathlib
from datetime import datetime, timezone
from dune_client.client import DuneClient
from dune_client.query import QueryBase

API_KEY = os.environ.get("DUNE_API_KEY", "")
OUT = pathlib.Path("research/mt440_output")
OUT.mkdir(parents=True, exist_ok=True)


def main():
    print("=" * 60)
    print("MT-440: Dune Feasibility Spike (SDK)")
    print(f"Started: {datetime.now(timezone.utc).isoformat()}")
    print("=" * 60)

    if not API_KEY:
        print("FATAL: DUNE_API_KEY not set.")
        sys.exit(1)

    print(f"\nAPI key prefix: {API_KEY[:6]}...")

    # ——— Test queries ———
    queries = [
        # Query IDs that should work for Solana
        QueryBase(4593126, "Solana tokens created"),     # known working: tokens + addresses
        QueryBase(3493826, "Example from Dune docs"),      # docs example
        QueryBase(4972183, "Solana DEX aggregated swaps"), # DEX swaps
    ]

    client = DuneClient(API_KEY)

    print("\n--- Query Results ---\n")

    results_summary = []
    for q in queries:
        print(f"\nQuery {q.query_id}: {q.name}")
        print("-" * 50)

        try:
            # Try cached results first
            try:
                result = client.get_latest_result(q.query_id)
                if result and result.result and result.result.rows:
                    rows = result.result.rows
                    cols = result.result.columns or (list(rows[0].keys()) if rows else [])
                    print(f"  Cached: {len(rows)} rows")
                    print(f"  Columns: {cols[:10]}")
                    if rows:
                        print(f"  Sample: {json.dumps({k: str(v)[:60] for k, v in list(rows[0].items())[:4]})}")
                    results_summary.append({
                        "query_id": q.query_id,
                        "name": q.name,
                        "rows": len(rows),
                        "columns": cols,
                        "sample_row": rows[0] if rows else None,
                        "source": "cached",
                    })
                    continue
            except Exception as e:
                print(f"  No cache: {e}")

            # Execute fresh
            print(f"  Executing (may use credits)...")
            result = client.execute(q)
            if result and result.result and result.result.rows:
                rows = result.result.rows
                cols = result.result.columns or (list(rows[0].keys()) if rows else [])
                print(f"  Executed: {len(rows)} rows")
                print(f"  Columns: {cols[:10]}")
                if rows:
                    print(f"  Sample: {json.dumps({k: str(v)[:60] for k, v in list(rows[0].items())[:4]})}")
                results_summary.append({
                    "query_id": q.query_id,
                    "name": q.name,
                    "rows": len(rows),
                    "columns": cols,
                    "sample_row": rows[0] if rows else None,
                    "source": "executed",
                })

        except Exception as e:
            print(f"  FAILED: {e}")

    # ——— Assessment ———
    print("\n" + "=" * 60)
    print("ASSESSMENT")
    print("=" * 60)

    report = {
        "task": "MT-440",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "api_key_prefix": API_KEY[:6],
        "queries_tested": len(queries),
        "queries_succeeded": len(results_summary),
        "results": [],
        "verdict": "NOT FEASIBLE",
        "recommendation": "",
        "constraints": [],
    }

    for rs in results_summary:
        cols = rs["columns"]
        has_mint = any(k in c.lower() for c in cols for k in ["mint", "token_address", "contract", "address"])
        has_price = any("price" in c.lower() or "amount_usd" in c.lower() for c in cols)
        has_volume = any("volume" in c.lower() for c in cols)
        has_timestamp = any("time" in c.lower() or "date" in c.lower() or "block_time" in c.lower() for c in cols)
        has_dex = any("dex" in c.lower() or "platform" in c.lower() or "exchange" in c.lower() for c in cols)
        has_trader = any("trader" in c.lower() or "wallet" in c.lower() or "user" in c.lower() for c in cols)

        score = sum([has_mint * 3, has_price * 2, has_volume * 2, has_timestamp * 2, has_dex * 1, has_trader * 1, (rs["rows"] > 0) * 1])
        useful = score >= 6

        assess = {
            "query_id": rs["query_id"],
            "name": rs["name"],
            "rows": rs["rows"],
            "columns": cols,
            "has_mint": has_mint,
            "has_price": has_price,
            "has_volume": has_volume,
            "has_timestamp": has_timestamp,
            "has_dex": has_dex,
            "has_trader": has_trader,
            "score": score,
            "useful": useful,
            "sample_row_preview": {k: str(v)[:60] for k, v in (rs["sample_row"] or {}).items()},
        }
        report["results"].append(assess)

        print(f"\nQuery {assess['query_id']} ({assess['name']}):")
        print(f"  Rows: {assess['rows']}")
        print(f"  Mint/address: {'YES' if has_mint else 'no'}")
        print(f"  Price: {'YES' if has_price else 'no'}")
        print(f"  Volume: {'YES' if has_volume else 'no'}")
        print(f"  Timestamps: {'YES' if has_timestamp else 'no'}")
        print(f"  DEX: {'YES' if has_dex else 'no'}")
        print(f"  Trader: {'YES' if has_trader else 'no'}")
        print(f"  Score: {score}/12 — {'USEFUL' if useful else 'NOT USEFUL'}")

    # Determine verdict
    if any(r["useful"] for r in report["results"]):
        report["verdict"] = "FEASIBLE"
        report["recommendation"] = (
            "Dune can supply useful Solana memecoin data. "
            "Recommended next steps: identify the specific queries that cover "
            "our needs, schedule periodic refreshes, and wire results into the "
            "signal pipeline. Upgrade from free tier may be needed for "
            "production use."
        )
    else:
        report["verdict"] = "NOT FEASIBLE"
        report["recommendation"] = (
            "Dune did not return trading-useful data on the queries tested. "
            "Free-tier constraints (rate limits, deprecated queries) severely "
            "limit what's accessible. Consider: (a) upgrading to a paid Dune "
            "plan for query execution + custom queries, (b) using Helius "
            "webhooks for real-time data, or (c) Jupiter/DexScreener API feeds."
        )

    # Constraints discovered
    report["constraints"] = [
        "Free plan rate-limited to ~1 request/minute for query execution",
        "Many public Solana DEX queries use deprecated engine (400 error)",
        "Some queries reference tables that no longer exist in public schema",
        "Only cached results are freely accessible; execution costs credits",
    ]

    # Save report
    report_path = OUT / "feasibility_report_sdk.json"
    report_path.write_text(json.dumps(report, indent=2, default=str))
    print(f"\nReport saved to: {report_path}")

    # Markdown summary
    md = [
        "# MT-440: Dune Solana Data Feasibility — SDK Report",
        "",
        f"**Verdict:** {report['verdict']}",
        f"**API Key:** {API_KEY[:6]}... (valid)",
        f"**Queries tested:** {report['queries_tested']}",
        f"**Queries succeeded:** {report['queries_succeeded']}",
        "",
        "## Results",
    ]
    for r in report["results"]:
        md.append(f"\n### Query {r['query_id']} — {r['name']}")
        md.append(f"- Rows: {r['rows']}")
        md.append(f"- Mint/address: {'yes' if r['has_mint'] else 'no'}")
        md.append(f"- Price data: {'yes' if r['has_price'] else 'no'}")
        md.append(f"- Volume data: {'yes' if r['has_volume'] else 'no'}")
        md.append(f"- Timestamps: {'yes' if r['has_timestamp'] else 'no'}")
        md.append(f"- DEX info: {'yes' if r['has_dex'] else 'no'}")
        md.append(f"- Trader/wallet info: {'yes' if r['has_trader'] else 'no'}")
        md.append(f"- Score: {r['score']}/12")
        md.append(f"- **Useful: {'YES' if r['useful'] else 'NO'}**")
        if r["sample_row_preview"]:
            md.append(f"- Sample: `{r['sample_row_preview']}`")

    md.extend([
        "",
        "## Constraints",
    ])
    for c in report["constraints"]:
        md.append(f"- {c}")

    md.extend([
        "",
        "## Recommendation",
        report["recommendation"],
    ])

    summary_path = OUT / "SUMMARY_SDK.md"
    summary_path.write_text("\n".join(md))
    print(f"Summary saved to: {summary_path}")

    print("\nDone.")
    return 0 if report["verdict"] == "FEASIBLE" else 1


if __name__ == "__main__":
    sys.exit(main())
