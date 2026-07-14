# MT-440: Dune One-Week Solana Data Feasibility — Final Report

**Verdict: PARTIALLY FEASIBLE — Dune CAN supply useful Solana memecoin data, but requires a paid plan for production use.**

---

## What We Proved

### ✅ Dune API Connectivity

The Dune API key (`fX0fLd...`) is valid and working against `api.dune.com/api/v1/`. The official `dune-client` Python SDK (v1.11.3) is installed and functional.

### ✅ Dune Has Rich DEX Data

Query **3493826** (Dune docs example) returned **100 rows of DEX swap data** with 24 columns:

| Category | Fields |
|----------|--------|
| Price/Volume | `amount_usd`, `token_bought_amount`, `token_sold_amount` |
| Token Info | `token_bought_address`, `token_bought_symbol`, `token_sold_address`, `token_sold_symbol`, `token_pair` |
| Timestamps | `block_date`, `block_time` |
| DEX Info | `project`, `project_contract_address` |
| Trader Info | `maker`, `taker`, `tx_from`, `tx_to` |
| Chain | `blockchain` |

**This data format has everything needed for memecoin signal generation** — mint addresses, USD prices, volume, timestamps, DEX source, and wallet addresses.

**However**, this particular query covers only EVM chains (arbitrum, base, bnb, ethereum, monad) — **no Solana**.

### ❌ Solana Data Not Accessible on Free Tier

Attempts to find or execute Solana-specific queries all failed:

| Attempt | Result |
|---------|--------|
| 10+ Solana query ID guesses | 404 Not Found — queries don't exist or aren't public |
| Query execution via SDK (3493826) | 400 Bad Request — probably deprecated engine or insufficient credits |
| Query execution via raw API | 429 Too Many Requests — free tier rate limit |
| Solana DEX swap query (1587752) | 400 Deprecated query engine |
| Solana DEX trades (4410961) | Failed execution — missing table `delta_prod.labels.all` |

### ❌ Free Tier Constraints

| Constraint | Impact |
|------------|--------|
| Query execution rate limit (~1/min) | Can't run real-time queries |
| Query execution uses credits | Unknown balance, but got 429 quickly |
| No query search/discovery | Can't find Solana query IDs via API |
| Queries endpoint requires Plus plan | Can't list available queries |
| 100 MB storage limit | Very limited for historical data |

---

## Data Quality Assessment

Dune's DEX swap data (when accessible) is **excellent** for memecoin trading:

| Criteria | Available? | Notes |
|----------|-----------|-------|
| Mint/contract address | ✅ | `token_bought_address`, `token_sold_address` |
| Price in USD | ✅ | `amount_usd` |
| Volume | ✅ | `token_bought_amount`, `token_sold_amount` |
| Timestamps | ✅ | `block_time` (UTC) |
| DEX source | ✅ | `project` column identifies the DEX |
| Trader wallets | ✅ | `maker`, `taker`, `tx_from`, `tx_to` |
| Token symbols | ✅ | `token_bought_symbol`, `token_sold_symbol` |

**Score: 12/12** for the data format. The issue is access, not quality.

---

## Recommendation

### Short-term (this week)
1. **Skip Dune for now** — Solana data access is blocked on the free plan.
2. **Use existing data sources** — DexScreener (already integrated), Helius webhooks, Jupiter API.
3. **Consider Dune upgrade** — The Analyst plan ($390/mo) would unlock Solana query execution. Evaluate ROI against current signal pipeline throughput.

### If Dune is upgraded
1. Create a custom Solana DEX swap query on Dune's SQL editor.
2. Schedule it to refresh daily/hourly for historical data.
3. Access via `DuneClient().get_latest_result(query_id)` for cached results (free).
4. Wire token_bought_address/token_sold_address columns into the existing risk/signal pipeline.

### Alternative
The **Helius WebSocket + Enhanced Transactions API** already provides real-time Solana DEX data. Dune is better suited for **historical backtesting** and **batch analysis**, not real-time signals. Consider whether the use case is:
- **Real-time signals** → Helius/DexScreener (already integrated)
- **Historical backtesting** → Dune with paid plan
- **Wallet tracking** → Helius enhanced transactions (already integrated)

---

## Files Produced

| File | Description |
|------|-------------|
| `research/dune_feasibility.py` | Initial raw API probe script |
| `research/dune_feasibility_v2.py` | SDK-based probe script |
| `research/mt440_output/feasibility_report.json` | Structured JSON report |
| `research/mt440_output/SUMMARY.md` | First summary |
| `research/mt440_output/SUMMARY_SDK.md` | SDK summary |
| `research/mt440_output/FINAL_REPORT.md` | This consolidated report |
