# MT-442: MELT/MemeTrans Historical Data Shape & Helius Enrichment Fit

**Date:** 2026-07-14
**Scope:** Survey all available historical memecoin data within `memecoin-trader`, assess shape fitness for pump/crash pattern research, define smallest useful Helius enrichment plan.

---

## 1. What Historical Data Exists

### 1.1 SQLite Database (`data/trades.db`, 4.2 MB)

| Table | Rows | Date Range | Description |
|-------|------|-----------|-------------|
| `trades` | 247 | 2026-07-08 → 2026-07-14 | Paper-only simulated BUY trades |
| `positions` | 32 | 2026-07-08 → 2026-07-14 | 19 closed, 13 open; 14 with close_price |
| `paper_decisions` | 655 | 2026-07-10 → 2026-07-12 | Candidate evaluation outcomes |
| `paper_soak_runs` | 28 | 2026-07-10 → 2026-07-12 | Paper cycle run summaries |
| `live_candidate_observations` | 15 | 2026-07-13 | Aggregated per-mint observation snapshots |

**Critical limitations for pump/crash research:**
- **No real on-chain tx_signatures** — all 247 trades have null tx_signature (paper simulates only)
- **No SELL trades** — 247 BUY, 0 SELL. No close-side order flow recorded as trades.
- **21 trades have real DexScreener-quoted prices** (not the default 1.0 SOL placeholder). Most are 0.01 SOL momentum-lane entries from recent MT-4xx runs.
- **241 distinct mints** across 247 trades, mostly pump.fun `.pump` tokens
- **All paper mode** — no live-mode data at all
- **Source skew** — 230/247 trades come from PUMP_FUN signal source

### 1.2 Real-Time Sources (No Historical Archive)

| Source | Endpoint | What We Capture | Persistence |
|--------|----------|----------------|-------------|
| Helius Enhanced Tx | `/v0/addresses/{addr}/transactions` | Wallet buys in current poll cycle | In-memory dedupe only |
| DexScreener | `token-profiles/latest/v1`, `/latest/dex/tokens/{mint}` | Token profiles + pair snapshots | In-memory snapshot delta |
| PumpPortal | WebSocket `subscribeNewToken` / `subscribeMigration` | New launches + graduations | In-memory dedupe |
| Twitter/X | Search API | `$TICKER` + mint mentions | In-memory dedupe |

**None of these sources persist raw payloads.** They emit `Signal` objects that enter the aggregator, get evaluated, and the evaluation result is saved as `paper_decisions`, but the raw provider response (transactions, pairs, etc.) is lost after the poll cycle.

### 1.3 External (Dune — Already Assessed MT-440)

Dune has rich Solana DEX swap data but requires a paid plan ($390/mo Analyst tier) to execute custom queries. The free-tier probe confirmed API connectivity and data quality but found zero actionable Solana queries accessible on the free plan.

### 1.4 Summary: Data Shape for Pump/Crash Research

The "MELT/Meme Trans" shape needed for pump-and-crash pattern research typically requires:

| Field | Needed For | Status in Current Data |
|-------|-----------|----------------------|
| `mint_address` | Token identity | ✅ Present in all tables |
| `price_sol` (time series) | Price action, pump detection | ⚠️ 21 entries have real DexScreener quotes; rest are placeholder 1.0 |
| `volume_5m / volume_1h` | Volume spike detection | ⚠️ Captured by OnChainMonitor in memory, not persisted historically |
| `buys_m5 / sells_m5` | Order flow imbalance | ⚠️ Same — ephemeral in-memory only |
| `liquidity_usd` | Pool depth, crash severity | ⚠️ Ephemeral in-memory or in risk diagnostics |
| `holder_count / top10_holder_pct` | Concentration, distribution | ⚠️ Captured in risk scorer but not persisted as time series |
| `tx_signature` | On-chain proof, replay | ❌ All null — paper-only |
| `pair_created_at` | Token age | ⚠️ Available in DexScreener snapshots but not persisted |
| `wallet_addresses` (traders) | Whale tracking, wallet clustering | ⚠️ Helius wallet polling captures current buys, no history |
| `funding_source` | Bundled launch detection | ⚠️ `funding_analysis.py` computes this live but doesn't persist |

**Verdict: Current persisted data is insufficient for MELT/MemeTrans-style pump/crash research.** The database is a decision/paper-trade ledger, not a historical market data archive. Critical missing dimensions: price time series, volume/seLL order flow, holder concentration over time, and on-chain transaction proofs.

---

## 2. What Helius Can Enrich (Smallest Useful Plan)

### 2.1 Helius Endpoints Already Used

| Endpoint | Used By | Purpose |
|----------|---------|---------|
| `GET /v0/addresses/{address}/transactions` | `whale_tracker.py` | Poll tracked wallet transactions |
| Helius RPC (`mainnet.helius-rpc.com`) | `helius_providers.py` | `getBalance`, `getTokenAccountsByOwner`, `simulateTransaction` |

### 2.2 Helius Endpoints NOT Yet Used (Enrichment Candidates)

| Endpoint | What It Provides | Value for Pump/Crash Research |
|----------|-----------------|------------------------------|
| `GET /v0/tokens/{mint}/metadata` | Token name, symbol, URI, creators | ⭐ Confirms identity, detects spoof tokens |
| `GET /v0/addresses/{address}/transactions?before={sig}` | **Paginated** historical transaction history | ⭐⭐⭐ Backfill whale wallet history |
| `GET /v0/token-metadata` (batch) | Bulk token metadata | ⭐ Enrich all 241 persisted mints |
| DAS: `getAsset` | Full digital asset info (creators, authorities, collections) | ⭐⭐⭐ Detect creator clusters, authority patterns |
| DAS: `getTokenAccounts` | Token holder distribution | ⭐⭐⭐ Historical holder snapshot |
| WebSocket: Transaction subscription | Real-time per-mint tx stream | ⭐ Real-time pump detection |
| Webhooks API | Configure persistent event stream | ⭐⭐ Better than polling for real-time |

### 2.3 Smallest Useful Helius Enrichment Plan

**Phase 1 — Static Enrichment (no runtime changes, offline script)**

1. **Backfill token metadata** for all 241 mints via `GET /v0/tokens/{mint}/metadata`
   - Creates `token_metadata` table: mint, name, symbol, creators, uri, on-chain metadata
   - Enables: identity verification, creator clustering, spoof detection

2. **Paginate Helius enhanced transactions** for tracked wallets
   - The current `whale_tracker.py` polls most recent 25 with no pagination
   - Add `before={last_signature}` loop to pull full history per wallet
   - Creates `wallet_tx_history` table: signature, mint, amount, timestamp, type
   - Enables: whale behavior patterns, entry/exit timing analysis

3. **Harvest DexScreener historical quotes** for persisted mints
   - Re-call `GET /latest/dex/tokens/{mint}` for each of 241 mints
   - Records current price, volume, liquidity as a time-series row
   - Creates `price_snapshots` table: mint, price_sol, volume_h24, liquidity_usd, timestamp
   - Enables: price change tracking, current market context for past decisions

**Phase 2 — Persistence Wiring (runtime changes)**

4. **Persist raw `PairSnapshot` data** in `OnChainMonitor`
   - Currently ephemeral in-memory, lost after poll cycle
   - Write snapshot to `price_snapshots` table on each poll
   - Enables: continuous time-series for active tokens

5. **Persist risk assessment details** in `RiskAssessment`
   - Currently `paper_decisions.diagnostics_json` stores capped text
   - Add dedicated `risk_snapshots` table: mint, holder_pct, liquidity, authority state, timestamp
   - Enables: correlation between risk profile changes and price action

6. **Collect `funding_analysis` results** as persisted rows
   - Currently computed live and discarded
   - Add `funding_snapshots` table: mint, bundled_buyer_pct, funder_count, timestamp
   - Enables: sybil/bundled launch detection over time

**Phase 3 — Historical Reconstruction (optional)**

7. **Re-run DexScreener + RugCheck** for all 241 mints on a schedule
   - Creates a forward-looking time series with daily snapshots
   - After 30+ days: enough data points for pump/crash pattern analysis
   - Without Dune: this is the most practical way to build historical depth

### 2.4 What NOT to Build Now

| Item | Why Not |
|------|---------|
| Dune paid plan ($390/mo) | Cost exceeds current research value. Revisit if signals pipeline generates consistent profit. |
| Helius Webhooks | Requires a public callback endpoint. Overhead > polling for current scale. |
| Raw transaction persistence | 241 mints × 100s of tx each = thousands of API calls. Start with metadata + quotes. |
| SELL trade recording | Paper close doesn't produce a trade row. Would need `paper-close` wiring change. |

---

## 3. Data Shape Recommendation

For pump/crash research, the minimum viable "MemeTrans" schema is:

```sql
CREATE TABLE token_metadata (
    mint_address TEXT PRIMARY KEY,
    name TEXT,
    symbol TEXT,
    creator_address TEXT,
    uri TEXT,
    created_at TEXT,
    first_seen_at TEXT,
    metadata_fetched_at TEXT
);

CREATE TABLE price_snapshots (
    id TEXT PRIMARY KEY,
    mint_address TEXT NOT NULL,
    price_sol REAL,
    price_usd REAL,
    volume_h24 REAL,
    liquidity_usd REAL,
    fdv_usd REAL,
    pair_address TEXT,
    observed_at TEXT NOT NULL
);

CREATE TABLE wallet_tx_history (
    signature TEXT PRIMARY KEY,
    wallet_address TEXT NOT NULL,
    mint_address TEXT NOT NULL,
    token_amount REAL,
    sol_amount REAL,
    type TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    source TEXT NOT NULL
);

CREATE TABLE risk_snapshots (
    id TEXT PRIMARY KEY,
    mint_address TEXT NOT NULL,
    holder_count INTEGER,
    top10_holder_pct REAL,
    liquidity_sol REAL,
    mint_authority_revoked INTEGER,
    freeze_authority_revoked INTEGER,
    is_honeypot INTEGER,
    risk_score REAL,
    observed_at TEXT NOT NULL
);
```

This gives us the four data planes needed for pump/crash analysis:
1. **Identity** (token_metadata) — who created it, when, what is it
2. **Price/Volume** (price_snapshots) — the market action time series
3. **Wallet Flow** (wallet_tx_history) — who bought/sold and when
4. **Risk Profile** (risk_snapshots) — concentration, authorities, safety signals

---

## 4. Files Produced

| File | Description |
|------|-------------|
| `research/mt442_output/RESEARCH_REPORT.md` | This report — full data shape analysis + enrichment plan |

