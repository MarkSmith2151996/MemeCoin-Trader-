# MT-440: Dune Solana Data Feasibility — SDK Report

**Verdict:** NOT FEASIBLE
**API Key:** fX0fLd... (valid)
**Queries tested:** 3
**Queries succeeded:** 0

## Results

## Constraints
- Free plan rate-limited to ~1 request/minute for query execution
- Many public Solana DEX queries use deprecated engine (400 error)
- Some queries reference tables that no longer exist in public schema
- Only cached results are freely accessible; execution costs credits

## Recommendation
Dune did not return trading-useful data on the queries tested. Free-tier constraints (rate limits, deprecated queries) severely limit what's accessible. Consider: (a) upgrading to a paid Dune plan for query execution + custom queries, (b) using Helius webhooks for real-time data, or (c) Jupiter/DexScreener API feeds.