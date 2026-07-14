#!/usr/bin/env python3
"""MT-440: Dune one-week Solana data feasibility spike.

Single-shot probe — tests Dune API connectivity, executes a relevant
Solana DEX query, fetches results, and assesses the data's usefulness
for memecoin trading signal generation.
Output is written to the task output directory as a structured report.
"""

import os
import json
import time
import sys
import pathlib
from datetime import datetime, timezone
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

API_KEY = os.environ.get("DUNE_API_KEY", "")
BASE = "https://api.dune.com/api/v1"

# ——— Known Solana DEX queries (public) ———
# We try each in order; first to return meaningful data wins.
SOLANA_QUERIES = [
    # Token metadata queries (good for reference)
    {
        "id": 4593126,
        "label": "Solana Token Creation & Trading",
        "description": "New token addresses and names",
    },
    # Known public Solana DEX swap queries
    {
        "id": 1587752,
        "label": "Solana DEX Swaps (full)",
        "description": "Comprehensive Solana DEX swap data",
    },
    {
        "id": 4410961,
        "label": "Solana DEX Trades",
        "description": "Solana DEX trade data with price/volume",
    },
    # Solana memecoin-specific queries
    {
        "id": 4889296,
        "label": "Solana New Tokens (bounded)",
        "description": "Recently created Solana tokens with early DEX activity",
    },
    # Solana liquidity / volume queries
    {
        "id": 4862258,
        "label": "Solana Pool Liquidity",
        "description": "Solana DEX pool liquidity data",
    },
    # General Solana on-chain data
    {
        "id": 4936735,
        "label": "Solana Top Tokens by Volume",
        "description": "Top traded Solana tokens by 24h volume",
    },
    {
        "id": 4972183,
        "label": "Solana DEX Aggregated Swaps",
        "description": "Aggregated Solana DEX swap data with USD values",
    },
]


def _api(path: str, method: str = "GET", body: bytes | None = None,
         silent: bool = False) -> dict | None:
    url = f"{BASE}/{path.lstrip('/')}"
    headers = {"x-dune-api-key": API_KEY, "Content-Type": "application/json"}
    req = Request(url, data=body, headers=headers, method=method)
    try:
        with urlopen(req, timeout=60) as resp:
            raw = resp.read()
            return json.loads(raw) if raw else {}
    except HTTPError as e:
        detail = e.read().decode() if e.fp else ""
        if not silent:
            print(f"  HTTP {e.code} on {path}: {detail[:300]}")
        return {"_http_code": e.code, "_error": detail}
    except URLError as e:
        if not silent:
            print(f"  Network error on {path}: {e.reason}")
        return {"_http_code": 0, "_error": str(e.reason)}
    except Exception as e:
        if not silent:
            print(f"  Unexpected error on {path}: {e}")
        return {"_http_code": 0, "_error": str(e)}


def test_connectivity() -> bool:
    """Verify Dune API key works.

    Dune v1 — a 200 or a 404 with a meaningful body means key is valid.
    A 401 means the key is invalid.
    """
    print("  Checking API key connectivity ...")
    result = _api("query/1052190/results", silent=True)
    if result is None:
        print("    FATAL: Network/Crashing error")
        return False
    code = result.get("_http_code", 200)
    if code == 401:
        print(f"    INVALID — API key rejected (401)")
        return False
    if code == 404:
        # 404 means "no execution" which means key IS valid but query has no cache
        print(f"    OK — API key is valid (got 404 with meaningful error, not 401)")
        return True
    if code == 200:
        print(f"    OK — API key valid, query has cached results")
        return True
    print(f"    OK — API key appears valid (HTTP {code})")
    return True


def try_query(qinfo: dict) -> dict | None:
    """Try to get results for a cached query, or execute and poll."""
    qid = qinfo["id"]
    label = qinfo["label"]
    print(f"\n  [{qid}] {label}")
    print(f"    Fetching cached results ...")

    # Try cached results first
    result = _api(f"query/{qid}/results", silent=True)
    if result and "_http_code" not in result:
        # v1 format: {"execution_id": "...", "state": "...", "result": {"rows": [...], ...}}
        rows_container = result.get("result", {})
        if isinstance(rows_container, dict):
            rows = rows_container.get("rows", [])
        elif isinstance(rows_container, list):
            rows = rows_container
        else:
            rows = []
        if rows:
            print(f"    Cached hit — {len(rows)} rows")
            return result
        else:
            print(f"    Cached empty (0 rows)")
    elif result and result.get("_http_code") == 404:
        print(f"    No cached results (query never executed or not public)")
    else:
        code = result.get("_http_code", "?") if result else "?"
        print(f"    Could not fetch cached results (HTTP {code})")

    # Try to execute the query
    print(f"    Executing query ...")
    exec_body = json.dumps({"query_id": qid}).encode()
    exec_result = _api(f"query/{qid}/execute", method="POST", body=exec_body)
    if exec_result is None:
        return None

    execution_id = exec_result.get("execution_id")
    if not execution_id:
        print(f"    No execution_id. Response: {json.dumps(exec_result)[:200]}")
        return None

    # Poll for completion
    print(f"    Execution ID: {execution_id}")
    for attempt in range(30):
        time.sleep(2)
        status = _api(f"execution/{execution_id}/status", silent=True)
        if status is None or status.get("_http_code"):
            continue
        state = status.get("state", "")
        print(f"    status={state}  ({attempt*2}s)", end="\r")
        if state == "QUERY_STATE_COMPLETED":
            print(f"\n    Execution completed.")
            exec_results = _api(f"execution/{execution_id}/results", silent=True)
            if exec_results and "_http_code" not in exec_results:
                return exec_results
            break
        elif state in ("QUERY_STATE_FAILED", "QUERY_STATE_CANCELLED"):
            reason = status.get("error", status.get("reason", "unknown"))
            print(f"\n    Query failed: {reason}")
            return None

    print(f"\n    Timed out after ~60s.")
    return None


def assess_results(results: dict, query_label: str) -> dict:
    """Analyze results for memecoin trading usefulness."""
    # Debug first
    print(f"  [DEBUG] results keys: {list(results.keys())}")
    for k, v in results.items():
        if k == "result":
            if isinstance(v, dict):
                print(f"  [DEBUG] result keys: {list(v.keys())}")
                print(f"  [DEBUG] result type for 'columns': {type(v.get('columns'))}")
            else:
                print(f"  [DEBUG] result is type={type(v).__name__}, preview={str(v)[:200]}")

    meta = results.get("result", {})
    if isinstance(meta, dict):
        rows = meta.get("rows", [])
        # Columns from metadata
        md = meta.get("metadata", {})
        col_names = md.get("column_names", [])
        if col_names:
            columns = col_names
        elif rows:
            columns = list(rows[0].keys())
        else:
            columns = []
    elif isinstance(meta, list):
        rows = meta
        columns = list(rows[0].keys()) if rows else []
    else:
        rows = []
        columns = []

    if rows:
        sample = rows[0]
        if isinstance(sample, dict):
            print(f"  [DATA] Columns: {list(sample.keys())}")
            print(f"  [DATA] Sample: {json.dumps({k: str(v)[:80] for k, v in list(sample.items())[:5]}, default=str)}")

    # Check for mint-like addresses in sample data (column named "address" often means token mint)
    sample_has_mint = False
    if rows and "address" in columns:
        addr_idx = columns.index("address")
        sample_val = str(rows[0].get("address", "")) if isinstance(rows[0], dict) else ""
        sample_has_mint = len(sample_val) > 30 and not sample_val.startswith("http")

    assessment = {
        "query_label": query_label,
        "total_rows": len(rows),
        "columns": columns,
        "has_mint_address": any("mint" in c.lower() or "token" in c.lower() or "contract" in c.lower() for c in columns) or sample_has_mint,
        "has_address_column": "address" in columns,
        "has_price": any("price" in c.lower() or "amount" in c.lower() or "value" in c.lower() for c in columns),
        "has_volume": any("volume" in c.lower() or "amount" in c.lower() for c in columns),
        "has_timestamp": any("time" in c.lower() or "date" in c.lower() or "block" in c.lower() for c in columns),
        "has_dex_info": any("dex" in c.lower() or "platform" in c.lower() or "exchange" in c.lower() for c in columns),
        "has_trader_info": any("trader" in c.lower() or "user" in c.lower() or "wallet" in c.lower() or "account" in c.lower() for c in columns),
        "sample_rows": rows[:3] if rows else [],
        "useful_for_memecoin": False,
    }

    # Criteria for usefulness
    score = 0
    if assessment["has_mint_address"]:
        score += 3
    if assessment["has_price"]:
        score += 2
    if assessment["has_volume"]:
        score += 2
    if assessment["has_timestamp"]:
        score += 2
    if assessment["has_dex_info"]:
        score += 1
    if assessment["has_trader_info"]:
        score += 1
    if assessment["total_rows"] > 0:
        score += 1

    assessment["usefulness_score"] = score
    assessment["useful_for_memecoin"] = score >= 6

    return assessment


def main():
    print("=" * 60)
    print("MT-440: Dune Solana Data Feasibility Spike")
    print(f"Started: {datetime.now(timezone.utc).isoformat()}")
    print("=" * 60)
    print()

    # Output directory
    out_dir = pathlib.Path("research/mt440_output")
    out_dir.mkdir(parents=True, exist_ok=True)

    # ——— Step 1: Connectivity ———
    print("Step 1: API Connectivity")
    print("-" * 40)
    if not API_KEY:
        print("  FATAL: DUNE_API_KEY not set.")
        sys.exit(1)
    connected = test_connectivity()
    print()

    if not connected:
        print("  Aborting — API key not valid.")
        sys.exit(1)

    # ——— Step 2: Try queries ———
    print("Step 2: Query Execution")
    print("-" * 40)
    results_by_query = {}
    for qinfo in SOLANA_QUERIES:
        print(f"\n  Trying query {qinfo['id']}: {qinfo['label']} ...")
        result = try_query(qinfo)
        if result and result.get("result", {}).get("rows"):
            results_by_query[qinfo["id"]] = {
                "info": qinfo,
                "results": result,
            }
            print(f"  SUCCESS — {len(result['result']['rows'])} rows returned.")
        elif result:
            print(f"  Query responded but no rows.")
        else:
            print(f"  No response for this query.")
        # small delay between queries to avoid rate limits
        time.sleep(1)

    # ——— Step 3: Assessment ———
    print("\nStep 3: Data Assessment")
    print("-" * 40)
    assessments = []
    for qid, data in results_by_query.items():
        assess = assess_results(data["results"], data["info"]["label"])
        assessments.append(assess)
        print(f"\n  Query: {data['info']['label']} ({qid})")
        print(f"  Rows: {assess['total_rows']}")
        print(f"  Columns ({len(assess['columns'])}): {', '.join(assess['columns'][:15])}")
        print(f"  Has mint address: {assess['has_mint_address']}")
        print(f"  Has price data: {assess['has_price']}")
        print(f"  Has volume data: {assess['has_volume']}")
        print(f"  Has timestamps: {assess['has_timestamp']}")
        print(f"  Has DEX info: {assess['has_dex_info']}")
        print(f"  Has trader info: {assess['has_trader_info']}")
        print(f"  Usefulness score: {assess['usefulness_score']}/12")
        print(f"  USEFUL FOR MEMECOIN: {'YES' if assess['useful_for_memecoin'] else 'NO'}")

    # ——— Step 4: Report ———
    print("\nStep 4: Generating Report")
    print("-" * 40)

    report = {
        "task": "MT-440",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "api_key_valid": connected,
        "api_key_prefix": API_KEY[:6] + "..." if API_KEY else "N/A",
        "queries_tried": len(SOLANA_QUERIES),
        "queries_succeeded": len(results_by_query),
        "assessments": assessments,
        "verdict": "FEASIBLE" if assessments and any(a["useful_for_memecoin"] for a in assessments) else "NOT FEASIBLE",
        "recommendation": "",
    }

    if report["verdict"] == "FEASIBLE":
        report["recommendation"] = (
            "Dune can supply useful Solana memecoin data. "
            "Next step: identify the specific query that best covers our needs, "
            "schedule periodic refreshes, and wire results into the signal pipeline."
        )
    else:
        report["recommendation"] = (
            "Dune did not return relevant data for our test queries. "
            "Consider: (a) trying custom queries on Dune, (b) using Helius webhooks "
            "for real-time data instead, or (c) building from DexScreener/Jupiter API feeds."
        )

    # Write report
    report_path = out_dir / "feasibility_report.json"
    report_path.write_text(json.dumps(report, indent=2, default=str))
    print(f"  Report written to: {report_path}")

    # Also write a human-readable summary
    summary_path = out_dir / "SUMMARY.md"
    lines = [
        "# MT-440: Dune Solana Data Feasibility — Summary",
        "",
        f"**Verdict:** {report['verdict']}",
        f"**API Key Valid:** {report['api_key_valid']}",
        f"**Queries Tried:** {report['queries_tried']}",
        f"**Queries Succeeded:** {report['queries_succeeded']}",
        "",
        "## Assessment",
    ]
    for a in assessments:
        lines.append(f"\n### {a['query_label']}")
        lines.append(f"- Rows: {a['total_rows']}")
        lines.append(f"- Columns: {', '.join(a['columns'][:10])}")
        lines.append(f"- Mint address: {'yes' if a['has_mint_address'] else 'no'}")
        lines.append(f"- Price data: {'yes' if a['has_price'] else 'no'}")
        lines.append(f"- Volume data: {'yes' if a['has_volume'] else 'no'}")
        lines.append(f"- Timestamps: {'yes' if a['has_timestamp'] else 'no'}")
        lines.append(f"- DEX info: {'yes' if a['has_dex_info'] else 'no'}")
        lines.append(f"- Trader/wallet info: {'yes' if a['has_trader_info'] else 'no'}")
        lines.append(f"- Score: {a['usefulness_score']}/12")
        lines.append(f"- **Useful: {'YES' if a['useful_for_memecoin'] else 'NO'}**")

    lines.extend([
        "",
        "## Recommendation",
        report["recommendation"],
        "",
        "## Sample Data (first row)",
    ])
    if assessments and assessments[0]["sample_rows"]:
        lines.append(f"```json\n{json.dumps(assessments[0]['sample_rows'][0], indent=2)}\n```")
    else:
        lines.append("No sample data available.")

    summary_path.write_text("\n".join(lines))
    print(f"  Summary written to: {summary_path}")

    print("\nDone.")
    return 0 if report["verdict"] == "FEASIBLE" else 1


if __name__ == "__main__":
    sys.exit(main())
