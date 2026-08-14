# Jupiter vs DexScreener 1:1 Field & Filter Parity Audit (MT-547)

Read-only audit. Goal: prove every field, filter, gate, and derived value that
`fetch_candidates()` and `screen_coin()` in `scripts/run_strategy_b.py` use
from DexScreener has an exact Jupiter equivalent — or flag gaps before a
discovery-source migration.

Source of truth: `scripts/run_strategy_b.py` (repo root — NOT `/workspace`,
which has no `data/` or `.env`; same path resolution as MT-539/MT-543/MT-545).
Jupiter data: live `GET https://api.jup.ag/tokens/v2/toporganicscore/5m?limit=5`
(and `limit=100`, `/search`) with `x-api-key` from `.env` (`JUPITER_API_KEY`),
captured 2026-08-14 22:16 UTC. DexScreener data: live `GET
https://api.dexscreener.com/latest/dex/search?q=...`, same window.

No code changed. No DB changed. No processes left running.

---

## 1. Every field read off the `coin` dict

### 1a. Inside `screen_coin()` (lines 514-685)

| Field path | Line | Used for | Type expected |
|---|---|---|---|
| `coin["mint"]` | 525 | RugCheck lookup key | str |
| `coin["created_timestamp"]` | 541 | age gate (epoch ms, `/1000` → seconds) | int/float epoch ms |
| `coin["usd_market_cap"]` | 549 | mcap min/max gates | float USD |
| `coin["pair"]` | 562 | must be a nested dict, else "no API pair attached" warning and all pair gates stay off | dict |
| `coin["pair"]["txns"]["h1"]["buys"]` | 564-565 | buy count for `txns` and `buy_sell_ratio` | int |
| `coin["pair"]["txns"]["h1"]["sells"]` | 564-566 | sell count for `txns` and `buy_sell_ratio` | int |
| `coin["pair"]["volume"]["h1"]` | 568 | `volume_pass` + `vol_mcap_pass` + est-fees | float USD |

Derived inside screen_coin (not read off `coin` but computed from the above):
- `txns = buys + sells` → `_age_adjusted_min_txns()` gate
- `bs_ratio = buys / max(sells, 1)` → `buy_sell_pass`
- `vol_ratio = vol / mcap` → `vol_mcap_pass` (bounds 0.005–50.0)
- `estimated_fees = txns * 0.001` vs `(mcap/15000) * 0.3` → `low_fees_warn`

**NOTE:** `screen_coin` writes `coin["rugcheck_report"] = report` (line 630) —
this is a write, not a read; downstream reads it from the same dict. It comes
from the RugCheck API, not from DexScreener, so it survives any source swap.

### 1b. Inside `_search_fresh_pair()` (lines 385-435) — the builder of the coin dict

| DexScreener API field | Line | Used for | Type |
|---|---|---|---|
| `pair["chainId"]` | 402 | must equal `"solana"` | str |
| `pair["quoteToken"]["address"]` | 404-405 | must equal `WRAPPED_SOL_MINT` | str |
| `pair["pairCreatedAt"]` | 407-410, 428 | age filter `<= SOURCE_MAX_AGE_MINUTES`, then `created_timestamp` | int epoch ms |
| `pair["baseToken"]["address"]` | 417-418 | `mint` | str |
| `pair["baseToken"]["symbol"]` | 426 | `ticker` | str |
| `pair["marketCap"]` / `pair["fdv"]` | 427 | `usd_market_cap` (mcap, fdv fallback) | float USD |
| `pair["txns"]["h1"]["buys"]` | 421-423 | `txns`, `buy_sell_ratio` | int |
| `pair["txns"]["h1"]["sells"]` | 421-423 | `txns`, `buy_sell_ratio` | int |
| `pair["volume"]["h1"]` | 429 | `volume` | float USD |
| `pair["liquidity"]["usd"]` | 432 | `liquidity` | float USD |
| `pair` (whole dict) | 433 | carried as `coin["pair"]` for screen_coin + downstream | dict |

The builder returns the coin dict with keys: `mint, ticker, usd_market_cap,
created_timestamp, volume, txns, buy_sell_ratio, liquidity, pair,
source_age_minutes`.

### 1c. Outside `screen_coin()` — all other `coin` reads

| Function | Field path | Line | Used for |
|---|---|---|---|
| `scan_loop` | `coin["ticker"]` | 1030 | logging, Grok query, entry |
| `scan_loop` | `coin["mint"]` | 1031 | seen_mints, entry |
| `scan_loop` | `coin["created_timestamp"]` | 1069-1070 | Grok `launched_at` (epoch ms `/1000`) |
| `scan_loop` | `coin.get("pair")` | 1128 | passed to `try_enter(pair=...)` |
| `log_candidate` | `coin["mint"]`, `coin.get("ticker")` | 732 | candidate_log insert |
| `log_candidate` | `coin.get("source_age_minutes")` | 711, 733 | `age_minutes` column |
| `log_candidate` | `coin.get("usd_market_cap")` | 711, 733 | `mcap_usd` column |
| `log_candidate` | `coin.get("buy_sell_ratio")` | 714, 735 | `buy_sell_ratio` column |
| `log_candidate` | `coin.get("rugcheck_report")` | 708 | rugcheck columns |
| `log_candidate` | `coin["pair"]["txns"]["h1"]["buys"]` | 707, 712, 734 | `txns_buys` |
| `log_candidate` | `coin["pair"]["txns"]["h1"]["sells"]` | 707, 712, 735 | `txns_sells` |
| `log_candidate` | `coin["pair"]["volume"]["h1"]` | 713, 734 | `volume_usd` |
| `log_candidate` | `coin["pair"]["liquidity"]["usd"]` | 736 | `liquidity_usd` |
| `log_candidate` | `coin["pair"]["fdv"]` | 736 | `fdv` |
| `log_candidate` | `coin["pair"]["priceUsd"]` | 737 | `price_usd` (float cast of string) |
| `log_candidate` | `coin["pair"]["priceChange"]["m5"]` | 738 | `price_change_5m` |
| `log_candidate` | `coin["pair"]["priceChange"]["h1"]` | 739 | `price_change_1h` |
| `try_enter` → `_pair_metadata` | `pair["pairCreatedAt"]` | 690-693 | `age_minutes` in trades.metadata_json |
| `try_enter` → `_pair_metadata` | `pair["marketCap"]` | 696 | trades metadata |
| `try_enter` → `_pair_metadata` | `pair["volume"]` | 696 | trades metadata |
| `try_enter` → `_pair_metadata` | `pair["txns"]` | 697 | trades metadata |
| `try_enter` → `_pair_metadata` | `pair["liquidity"]` | 697 | trades metadata |
| `try_enter` → `_pair_metadata` | `pair["fdv"]` | 698 | trades metadata |
| `try_enter` → `_pair_metadata` | `pair["priceUsd"]` | 699 | trades metadata |
| `try_enter` → `_pair_metadata` | `pair["priceChange"]` | 699 | trades metadata |

`monitor_loop` / `monitor_positions` never touch `coin` — they re-mark via
`DexScreenerPriceProvider.get_current_price(mint)` (execution price path, MT-538
era; independent of candidate discovery).

`parse_row()` (lines 354-382) builds a coin dict from raw browser-pc rows but
is **dead code** — no callsites in the file.

---

## 2. Jupiter field mapping (verified against live `toporganicscore/5m` + `/search`)

### MATCHED (1:1 equivalent exists)

| DexScreener field | Jupiter field | Transform needed | Unit match |
|---|---|---|---|
| `baseToken.address` → `mint` | `id` | none | str ✓ |
| `baseToken.symbol` → `ticker` | `symbol` | none | str ✓ |
| `baseToken.name` | `name` | none | str ✓ |
| `marketCap` / `fdv` → `usd_market_cap` | `mcap` / `fdv` | none; mcap can be `null` → same fdv fallback as DexScreener path | USD ✓ |
| `fdv` | `fdv` | none | USD ✓ |
| `pairCreatedAt` → `created_timestamp` | `firstPool.createdAt` | ISO-8601 → epoch ms (`datetime.fromisoformat().timestamp()*1000`) | epoch ms, second precision (see risk note) |
| `txns.h1.buys` | `stats1h.numBuys` | none | int count, same trailing-1h window ✓ |
| `txns.h1.sells` | `stats1h.numSells` | none | int count, same trailing-1h window ✓ |
| `volume.h1` | `stats1h.buyVolume + stats1h.sellVolume` | sum; verified values are USD (SOL 1h ≈ $160M) | USD ✓ |
| `liquidity.usd` | `liquidity` | none | USD ✓ (token-aggregate vs per-pair — see GAPS) |
| `priceUsd` | `usdPrice` | none (Jupiter is float, DexScreener is string) | USD ✓ |
| `priceChange.m5` | `stats5m.priceChange` | none | percent ✓ |
| `priceChange.h1` | `stats1h.priceChange` | none | percent ✓ |
| `txns.m5/h6/h24` (unused by B) | `stats5m/6h/24h.numBuys/numSells` | none | ✓ |
| `volume.m5/h6/h24` (unused) | `stats5m/6h/24h.buyVolume+sellVolume` | sum | USD ✓ |
| `chainId == "solana"` | n/a — Jupiter Tokens V2 is Solana-only | drop the check (trivially satisfied) | ✓ |
| holder-count telemetry (DexScreener lacks it) | `holderCount` | extra, not currently used | count ✓ |

### Unit-check answers (explicit)

- **`stats1h` window**: trailing 1 hour — same window as DexScreener `txns.h1` /
  `volume.h1`. `buyVolume + sellVolume` is USD (verified: Wrapped SOL stats1h
  buyVolume ≈ $79.8M + sellVolume ≈ $83.1M ≈ $162.9M total).
- **`stats1h.numBuys/numSells`**: trailing-1h transaction counts — same window
  and semantics as `txns.h1.buys/sells` (token-wide vs pair-wide, see GAPS).
- **`liquidity`**: Jupiter token-level aggregate USD liquidity (SOL ≈ $667M).
  Same currency, but aggregated across all pools rather than one pair.
- **`firstPool.createdAt`**: ISO string at **second precision** (e.g.
  `2026-08-14T21:28:14Z`). Epoch-ms → ISO conversion is lossless **only to the
  second**; sub-second ms are dropped. Irrelevant for minute-scale age gates.
- **`mcap` / `fdv`**: same circulating-supply × price calculation. Both USD.

---

## 3. GAPS (no direct equivalent)

| DexScreener field | Used for | Risk level | Workaround |
|---|---|---|---|
| **The whole nested `pair` dict** | every pair-gate + metadata (see STRUCTURAL section) | **HIGH** — every `coin["pair"]` access breaks | flatten Jupiter fields into the coin dict; rewrite all access paths |
| `quoteToken.address == WRAPPED_SOL_MINT` filter | ensures SOL-quoted pairs only | **HIGH** — Jupiter token records carry **no quote-token info**; `toporganicscore` mixes SOL/USDC/stables (USDC returned in top-5) | accept aggregate stats (SOL dominates fresh memecoins); or drop/soften the filter; no API-side quote filter exists |
| `pairCreatedAt` of the **newest** pool | `_search_fresh_pair` picks min-age pool per ticker | **MEDIUM** — Jupiter `firstPool.createdAt` is the **first/original** pool (pump.fun bonding curve), which can be older than the pool DexScreener search returns; verified divergence: PWC firstPool 21:28:14Z vs DexScreener pair 22:08:51Z (~40 min) — a coin passing DexScreener's ≤22m gate can fail Jupiter's | compute age from `firstPool.createdAt`; expect gate outcome drift vs DexScreener; revalidate gate stats after swap |
| `dexId` (e.g. `pumpswap`, `pumpfun`) | not read by screen_coin; useful context only | LOW | Jupiter has `launchpad` (e.g. `pump.fun`) + `graduatedPool`/`graduatedAt` — different but richer |
| `priceNative` | not read | LOW | no equivalent (Jupiter `usdPrice` only) |
| `info.*` (imageUrl, socials, websites) | not read in Strategy B | LOW | no equivalent (Jupiter has `icon`, `twitter`, `website` per-token) |
| `pairAddress` / `url` | not read | LOW | n/a — Jupiter pools endpoint not needed |
| `liquidity.base` / `liquidity.quote` | not read | LOW | n/a |

Fields read off `coin` that come from **RugCheck, not DexScreener** — unaffected
by the swap: `rugcheck_report` (provider_status, found, mint/freeze authority
revoked, top_holder_pct, creator pct).

### `resolve_mint()` (line 468) — callsite check

`resolve_mint()` is **dead code in run_strategy_b.py** — it is defined but never
called in the main loop or anywhere else in the file (grep: only the definition
and a warning log in `_search_fresh_pair` match). The active DexScreener search
path is `_search_fresh_pair()`, called from `fetch_candidates()` (line 1025)
every scan cycle — that is the callsite that must be replaced. (`run_paper_loop.py`
has its own independent `resolve_mint`; out of scope, still DexScreener-based.)

---

## 4. Settings / constants check (Step 4)

| Constant | Current behavior | Jupiter compatibility |
|---|---|---|
| `SOURCE_MAX_AGE_MINUTES` (=22) | filter in `_search_fresh_pair` line 411: `age_minutes <= SOURCE_MAX_AGE_MINUTES`; screen_coin re-checks via `GATES.max_age_minutes` | Works — compute age from `firstPool.createdAt`. Note the firstPool-vs-newest-pool semantic drift above. |
| `MAX_SOURCE_ROWS = 30` | tickers truncated to 30 before search | Jupiter `toporganicscore/5m?limit=30` returns 30 (verified 100 also works); limit param respected. One call covers it. |
| `WRAPPED_SOL_MINT` filter | DexScreener search filters `quoteToken.address == So1111...` | **GAP** — Jupiter token records have no quote-token field; no equivalent filter. Aggregate stats span all quote tokens. |
| `chainId == "solana"` | DexScreener search filters | **Safe** — Jupiter Tokens V2 is Solana-only; no cross-chain tokens leak in. Check can be dropped. |
| `MAX_AGE_MINUTES`/GATES | 22 min, mcap 5K-50K, vol ≥500, b/s ≥0.5 | All four map to Jupiter fields (Section 2). Age is the drift risk, not the mapping. |
| `seen_mints` TTL, `BLOCKED_UTC_HOURS`, sizing | mint-keyed, source-agnostic | Unaffected. |

Jupiter endpoint returns **all token ages** (Wrapped SOL, USDC, and fresh
pump.fun tokens all appear in `toporganicscore`), so the age gate does real
filtering work — same as DexScreener's API-age filter.

---

## 5. STRUCTURAL CHANGES NEEDED

Every place the nested `pair` dict is accessed directly (all in
`scripts/run_strategy_b.py`; line numbers current as of 2026-08-14):

| Location | Line(s) | Access |
|---|---|---|
| `_search_fresh_pair()` — builds the dict | 417-434 | `baseToken`, `pairCreatedAt`, `marketCap/fdv`, `volume.h1`, `txns.h1`, `liquidity.usd`, whole `pair` |
| `screen_coin()` — pair gates | 562-569 | `pair.txns.h1.buys/sells`, `pair.volume.h1` |
| `screen_coin()` — no-pair warning | 591-592 | `if not isinstance(pair, dict)` branch |
| `log_candidate()` — candidate_log insert | 706-707, 711-714, 732-739 | `pair.txns.h1`, `pair.volume.h1`, `pair.liquidity.usd`, `pair.fdv`, `pair.priceUsd`, `pair.priceChange.m5/h1`, `coin["mint"]`, `coin["ticker"]`, `source_age_minutes` |
| `scan_loop()` — Grok launch time | 1069-1070 | `coin["created_timestamp"]` |
| `scan_loop()` → `try_enter(pair=...)` | 1128 | passes `coin.get("pair")` |
| `_pair_metadata()` — trades.metadata_json | 688-701 | `pairCreatedAt`, `marketCap`, `volume`, `txns`, `liquidity`, `fdv`, `priceUsd`, `priceChange` |

### Migration shape (informational, no code changed)

A Jupiter-backed `fetch_candidates()` would build coin dicts with the same flat
keys (`mint, ticker, usd_market_cap, created_timestamp, volume, txns,
buy_sell_ratio, liquidity, source_age_minutes`) plus flattened `stats1h` values,
then either:
1. **Drop the `pair` key entirely** and rewrite `screen_coin`/`log_candidate`/
   `_pair_metadata` to read flat fields — every line in the table above changes; or
2. **Synthesize a `pair`-shaped dict** from Jupiter fields
   (`{"txns": {"h1": {"buys","sells"}}, "volume": {"h1"}, "liquidity": {"usd"},
   "fdv", "priceUsd", "priceChange": {"m5","h1"}, "pairCreatedAt"}`) so all
   existing access paths keep working — minimal diff, but `_pair_metadata`'s
   stored `"dexscreener"` metadata key would then be a misnomer.

RugCheck, Grok mentions, DexScreenerPriceProvider (execution marks), shadow
Jupiter quotes, and monitor loops are unaffected by the discovery-source swap.

---

## Appendix: source data provenance

- `GET https://api.jup.ag/tokens/v2/toporganicscore/5m?limit=5` (200; also
  `limit=30` → 30, `limit=100` → 100) and
  `GET https://api.jup.ag/tokens/v2/search?query={mint}` (200), header
  `x-api-key: jup_28b8…` from `.env` — 2026-08-14 22:16 UTC.
- `GET https://api.dexscreener.com/latest/dex/search?q=PigeonWifCig` (200,
  30 pairs) — same window; used to verify pair shape, `pairCreatedAt` values,
  and quote-token filtering.
- Cross-check with `data/jupiter_coverage_check.json` (MT-545, 150 mints):
  `mcap` can be `null` for indexed tokens (DICK: `mcap: null, fdv: null`),
  confirming the fdv-fallback requirement.
- Cross-check with `data/jupiter_v2_test.json` (MT-542): older snapshot with
  stats1h limited to `buyVolume/numBuys/numSells/sellVolume`; the live API now
  returns the full stats block (priceChange, numTraders, organic volumes, etc.).
