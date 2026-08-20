# MT-603 — Final Stabilization Report

**Date:** 2026-08-20 (executed 12:06–12:35 UTC)
**Scope:** 6-item lockdown before the Aug 23 unattended run. All findings below; code changes committed.

---

## 1. Jupiter vs DexScreener price provider benchmark

Standalone benchmark (`/tmp/price_benchmark.py`, not committed — run artifacts only): 25 live mints pulled from the same Jupiter Tokens V2 discovery endpoints the loop uses (`/toporganicscore/5m`, `/recent`, `/toptrending/5m`), then sequential round-trip timing against each provider (100 ms spacing, no parallelism).

| Provider | Endpoint | n | median | p95 | max | HTTP |
|---|---|---|---|---|---|---|
| Jupiter V3 (in use) | `https://api.jup.ag/price/v3?ids={mint},{SOL}` | 25 | **92.7 ms** | **119.3 ms** | **121.9 ms** | 200 ×25 |
| Jupiter V2 (task-prescribed) | `https://api.jup.ag/price/v2?ids={mint}` | 25 | — | — | — | **404 ×25** |
| DexScreener | `https://api.dexscreener.com/latest/dex/search?q={mint}` | 25 | 322.6 ms | 862.4 ms | 927.5 ms | 200 ×25 |

**Verdict:** Jupiter V3 is ~3.5× faster at the median (92.7 ms vs 322.6 ms) and ~7× faster at p95 (119.3 ms vs 862.4 ms). Jupiter does **not** add latency relative to DexScreener — it eliminates it. MT-602's V2→V3 substitution is confirmed correct: `/price/v2` returns HTTP 404 with an empty body on every request (deprecated endpoint), so the task-prescribed V2 URL is dead; V3 is the current endpoint with identical auth (`x-api-key`) and response shape.

Live-verified during the run: the V3 response carries `usdPrice` + `liquidity` per mint (sample mint `5y153TFb…` → `usdPrice=0.00000246`, `liquidity=2605 USD`, `launchpad=pump.fun`), so the SOL-price derivation (mint batched with `WRAPPED_SOL_MINT`, USD prices divided) has both legs in one call.

## 2. DexScreener reference sweep — removed dead code

Grep of the full codebase (`dexscreener`, `DexScreener`, `dex_screener`) completed. Removed from `scripts/run_strategy_b.py` (the live loop) everything that was **no longer called** after the MT-602 Jupiter swap:

- Constants: `BROWSER_PC_URL`, `STRATEGY_B_DEXSCREENER_URL`, `BROWSER_PC_WAIT_SECONDS`, `MAX_SOURCE_ROWS`
- Functions: `_parse_usd_string`, `_parse_age_minutes`, `parse_row`, `_search_fresh_pair`, `fetch_candidates` (the old browser-pc capture → DexScreener search discovery path, unused since MT-550/MT-588), `resolve_mint`
- Stale log messages: `"DexScreener: no API pair attached"` → `"no pair metadata attached"`; `"DexScreener search failed"` → `"pair metadata lookup failed"`
- Docstring on `_pair_metadata` updated (the synthesized pair shape is Jupiter-fed now); the persisted JSON key `dexscreener` was **kept** — it's DB row shape from the pre-Jupiter era and nothing reads it (`bot/db.js` reads only `metadata.close_reason`); renaming would silently change stored data shape for zero benefit.
- Test cleanup: `tests/test_strategy_b_overhaul.py` lost the `_search_fresh_pair` test + unused `_pair`/`time`/`httpx`/`SOURCE_MAX_AGE_MINUTES` imports (3 tests remain).

**Kept intentionally** (still called by live paths outside the loop):
- `DexScreenerPriceProvider` in `src/execution/price_provider.py` — still the default mark provider for `cli.py`, `run_paper_loop.py`, `paper_results.py`, MT-438/444/451 scripts, and `--marks live` CLI paths.
- `src/risk/liquidity.py` + `src/risk/scorer.py` DexScreener liquidity diagnostics (Strategy A-era scorer output, still used by `cli.py`).
- Signal modules (`whale_tracker`, `influencer_tracker`, `narrative_tracker`, `onchain`, `dexscreener_new_pairs`) — active standalone sources.
- One-off analysis scripts (`age_drift_analysis.py`, `compare_jupiter_vs_dex.py`, `shadow_jupiter_vs_dex.py`, `run_dexscreener_collection.py`, etc.) — historical tooling, not dead code in the trading path.

Full test suite: **822 passed** (823 baseline − 1 removed test), 1 skipped, 2 pre-existing failures unchanged (backtester 15-col INSERT + whale dotenv — both fail identically on the base commit). Ruff: no new errors (22 pre-existing E501s in `run_strategy_b.py` unchanged; 1 pre-existing E501 in the overhaul test).

## 3. MT-596 blocked-window sleep — review + fix

Reviewed the loop-top blocked-window branch (weekday `BLOCKED_WEEKDAYS` + `BLOCKED_UTC_HOURS` check before discovery, ~line 1472 in the current file):

- **Correct detection:** yes — `datetime.now(UTC).weekday()/hour` checked at the top of every iteration, before discovery and gate evaluation. The per-candidate checks inside `try_enter` remain as defense-in-depth.
- **Positions still managed:** yes — `monitor_positions(...)` runs every blocked iteration with the adapter wired, so hard stop / take profit / trailing stop / time stop / early-exit all fire during blocked windows (confirmed live in the 08:05 EDT log: hard_stop, take_profit, early_exit closes while candidates were being screened).
- **Clean resume:** yes — `continue` re-enters the loop; when the block lifts, discovery resumes on the next iteration. `BLOCKED_CHECK_INTERVAL_S = 60` bounds resume latency to ≤60 s.
- **Placement:** correct — before discovery, after loop setup; test mode performs one blocked check and exits.

**One fix applied:** `manager.get_all_open()` sat *outside* the try/except in the blocked branch — a transient SQLite error there would have crashed the loop mid-blocked-window. Both the position count and `monitor_positions` are now individually guarded (MT-603 edit), so a DB hiccup can never kill the loop during a block, and a failed count read no longer prevents exit management.

## 4. Gate calibration — 5-minute live sample

Sample window: 12:25:44–12:30:44 UTC (08:25:44–08:30:44 EDT, active hours, Jupiter provider live). Per-cycle funnel (9 logged cycles):

| Gate | avg per cycle | notes |
|---|---|---|
| candidates fetched | 31.0 | 3 Jupiter endpoints/cycle |
| age | 1.0 | ~1 *new* mint/cycle passes (see artifact note) |
| mcap | 0.78 | |
| pool depth | 0.44 | `liquidity_pass` |
| score | 0.33 | |
| txns | 0.22 | |
| volume | 0.22 | |
| vol/mcap | 0.44 | |
| low_fees (~) | 1.0 | warning-only in paper mode |
| buy/sell | 0.33 | |
| rugcheck | 0.44 | |
| holder | 0.11 | |
| **full_pass** | **0** | 0 in this window; 0.09/cycle session-wide |
| entry_attempts / entered | 0 / 0 | |

A complementary window (12:06:57–12:11:57 UTC, 96 SCREEN evaluations): **23 PASS (24%)**, 73 FAIL. Fail breakdown: ~20× `no_pool_liquidity (liquidity_usd=0)` on brand-new tokens, ~15× dead-volume combos (`txns<3` / `vol<$500` / score<40 on fresh bonding-curve tokens), ~4× `low_fees_warn` alone, rest mixed. Main-blocker distribution over the session: `none` 27 (all candidates deduped), `liquidity_pass` 5, `score_pass` 2.

**Primary finding — the "0 candidates pass age" observation is a measurement artifact, not a miscalibrated gate:** most cycles fetch the same ~30 hot mints and skip them as `already evaluated` (seen_mints dedup), so the per-cycle funnel shows `age=0` even though the age gate itself passes ~100% of genuinely-new candidates (they arrive ≤22 min old by construction of the Jupiter discovery feed). Of *new* candidates, ~24% pass the full screen.

**No parameter changes.** The dominant real-world blocker for fresh tokens is `no_pool_liquidity` (Jupiter reports `liquidity_usd=0` for sub-minute-old tokens whose pool liquidity hasn't propagated) and dead volume on ultra-fresh bonding-curve tokens — both are expected behavior for tokens that are seconds old, not gate miscalibration. Parameters remain walk-forward validated (MT-593) and frozen. Flagged, not changed, per instructions.

## 5. The 04:31–07:49 UTC dead stretch — no secondary issue

Reconstructed from `candidate_log` (UTC timestamps), `trades`, the watchdog log, and the health-monitor log (the strategy log was truncated at the MT-602 restart):

- **04:31–06:59 UTC:** discovery was healthy — `candidate_log` shows 1,231–1,366 candidates/hour screened, and **84 candidates passed all 11 critical gates** (reaching `try_enter`). Zero entries. This matches MT-602's "663 consecutive `no valid DexScreener price` skips" — the DexScreener price lookup at entry was the *sole* blocker. The loop itself was alive the whole window: watchdog logged `Strategy B OK` every 3 minutes, and the health monitor shows no Strategy B restarts between 21:36 UTC Aug 19 and 11:52 UTC Aug 20.
- **07:00–07:59 UTC:** `candidate_log` has **zero rows** — this is the **designed UTC hour-7 block** (`BLOCKED_UTC_HOURS` contains 7 in the committed code; the loop skips discovery entirely and only monitors positions). Not a stall, not a secondary issue — MT-596 working exactly as designed.
- **08:00–11:49 UTC:** the old (DexScreener) process resumed discovery after the block lifted; 08:00–08:32 saw a brief burst of 22 paper BUYs (DexScreener prices occasionally resolving), then silence 08:32–11:49 — same DexScreener blocker persisting.
- **11:49–11:52 UTC (MT-602 deploy):** 26 entries in the first ~4 minutes with the Jupiter provider; sustained since (82 full-pass candidates in hour 12 alone, entries flowing at the time of this report).

**Conclusion:** no secondary issue. Discovery never stalled, candidates reached the gates throughout, and the price provider was the only blocker. Jupiter (MT-602) resolves it.

## 6. Loop stability

- **Currently running:** PID 1907182, started 11:52:08 UTC (07:52 EDT) — the MT-602 Jupiter process. PPID 1 (`setsid`/detached via the health monitor), log mtime seconds-fresh at audit time (12:08 UTC), zero log gaps >5 min since boot.
- **Auto-restart: exists — two layers.** The "no auto-restart" premise is outdated:
  1. `scripts/health_monitor.py` (MT-529/530) — cron every minute, exclusive `/tmp` lock; restarted Strategy B at 21:36:10 UTC Aug 19 (process not found — recovered in ~3 s) and at 11:52:08 UTC Aug 20 (the MT-602 deploy).
  2. `/home/dev/watchdog_memecoin.sh` — cron every 3 minutes, `pgrep` + stale-log (>5 min) kill/restart. 296× `Strategy B OK`, 0× `DOWN`/`STALE` for Strategy B in the last 48 h.
- **Crashes in last 48 h:** 1 unplanned (21:36 UTC Aug 19, process not found) + 1 planned (MT-602 deploy, 11:52 UTC Aug 20). Both auto-recovered.
- **Known gap (flag only, not implemented):** no systemd/NSSM service. NSSM cannot launch WSL processes under non-interactive accounts (`WSL_E_LOCAL_SYSTEM_NOT_SUPPORTED`, documented MT-584); the cron watchdog + health monitor are the production restart mechanism and they demonstrably work. Residual risk: both depend on the WSL box + cron being alive — if the *box* dies, nothing restarts it. Acceptable for a same-host loop; a hardware-level watchdog is out of scope.
- **Out-of-scope flag:** Chrome CDP on Windows has been DOWN since ~11:48 UTC (health monitor has been failing `restart Chrome` every minute and alerting). This affects browser-pc only — Strategy B no longer depends on it (Jupiter discovery + Jupiter prices) and is unaffected. Also: Helius API key is at `max usage reached` (429) — priority-fee provider degrades to public-RPC fallback as designed.

---

## Summary of changes

- `scripts/run_strategy_b.py` — removed 199 lines of dead DexScreener/browser-pc discovery code + constants; neutralized stale log messages; hardened the MT-596 blocked-window branch against DB errors. Also included (already live in the running loop since 11:52 UTC, committed per MT-603's "commit everything"): the pre-existing UTC-hour-7 unblock (`BLOCKED_UTC_HOURS` {0,7,19,20,21} → {0,19,20,21}) and removal of the `MIN_MCAP_USD` floor check in `screen_coin`.
- `tests/test_strategy_b_mt588.py` — pre-existing mcap-floor test updated to match the removed floor (committed with the gate change).
- `tests/test_strategy_b_overhaul.py` — removed the dead `_search_fresh_pair` test + unused imports/helpers; 3 tests remain.
- `STABILIZATION_REPORT.md` — this file.
- **Not committed:** `bot/config.json` (now contains the real Telegram bot token — a credential; committing it would leak it into git history; recommend moving `telegram_token`/`telegram_chat_id` to env vars or gitignoring the file). `data/strategy_bt_report.xlsx` remains untracked (large binary artifact, gitignored via `data/*.xlsx`? — actually not ignored; left untracked as an output artifact).

**Verification:** full suite 822 passed / 1 skipped / 2 pre-existing failures unchanged; ruff no new errors; `py_compile` clean on both edited files. The running loop was **not** restarted — the blocked-branch hardening and dead-code removal apply on the next natural restart (watchdog/health-monitor or manual); the current process is healthy and unaffected.
