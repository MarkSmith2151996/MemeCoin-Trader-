# MT-616: Backtest-to-Live Divergence Diagnostic

**Date:** 2026-08-22
**Window:** live entries 2026-08-21 (1,284 entries; 1,224 matched to enriched parquet)
**Script:** `scripts/mt616_divergence_diagnostic.py`
**Outputs:** `data/matched_entries.csv`, `data/divergence_breakdown.csv`

## Verdict

**Both hypotheses are real, but H1 (gate input data mismatch) is the dominant
explainer of the same-day WR gap, with a structural entry-timing divergence
(the backtest has no 2-minute minimum-age deferral) as the largest per-token
lever. H2 (price mismatch) is a minor contributor (~12%).**

The 23-point headline gap (live 45.3% vs backtest 69.8%) is mostly **period
mix**: the backtest's 69.8% is a 122-day average. Run on Aug 21 alone, the same
backtest scores **51.98%** (MT-613 realistic) vs live's **45.34%** — a same-day
mechanism gap of **6.64 points**. On identical mints, the gap is 13.4 points,
decomposed below.

---

## 1. Headline numbers

| metric | live (Aug 21) | backtest MT-613 realistic (Aug 21 only) |
| --- | ---: | ---: |
| entries | 1,284 | 3,182 |
| win rate | 45.34% | 51.98% |
| SOL/day | +8.19 | +27.60 |
| WR gap (same day) | | **6.64 pts** |

The 122-day backtest average is 69.77% WR / +18.26 SOL/day (MT-613). Aug 21 was
below average for the backtest itself (51.98%), so **~17.8 points of the headline
23-point gap is calendar/market mix, not a mechanism difference**.

## 2. H1 — Gate input data mismatch (dominant same-day factor)

For every live entry we looked up the same mint at the same age in the enriched
parquet and re-ran the backtest gates (`capacity_sweep_bt.py:passes_gates`).

- **31.2% of live entries (382/1,224) would have FAILED the backtest gates at the
  same age** — Jupiter's snapshot data disagreed with the enriched on-chain data
  enough to flip the gate verdict.
- Live WR on those 382 would-be-rejected entries: **35.9%** (vs 49.6% on the
  entries the backtest would accept). Filtering them raises live WR from 45.34% →
  **49.64%** (+4.30 pts) — **65% of the same-day gap**.
- Failure breakdown (a mint can fail multiple gates):
  - **mcap: 327** — the dominant leak. Two sub-cohorts:
    - **212 entries where the parquet says mcap < $5.1K** (Jupiter inflated):
      live WR **24.1%** — the worst cohort. Jupiter's mcap overstatement admits
      dead tokens through the floor gate.
    - **113 entries where the parquet says mcap > $50K** (Jupiter deflated):
      live WR 58.4% — Jupiter's mcap understatement hid genuine breakouts
      (live actually profited from these; 19 parquet values >$1M are
      supply-encode artifacts, the rest 50-100K are borderline).
  - **pool depth: 177** — Jupiter `liquidity` converted to SOL vs parquet
    `max_sol_in_pool`. Live liquidity (mean $6,092) implied ≥5 SOL; parquet says
    the pool was shallower (many 0.2-4.3 SOL). Live WR on pool-only fails: 35.0%.
  - txns (12), volume (12), buy_sell (12), age (9), score (3), creator (1): minor.
- **Input-value divergence on the accepted cohort** (Jupiter/parquet ratios):
  mcap median 0.93 (Jupiter ~7% low), volume median 0.81 (~19% low), txns median
  0.76 (~24% low), buy/sell ratio median 1.09 but mean 7.32 (extreme tails).
  Jupiter systematically under-reports activity vs the enriched archive.

**Conclusion:** Jupiter's mcap/liquidity/volume/txns snapshot is *not* equivalent
to the on-chain enriched data the backtest gates on. The mcap gate is the biggest
single leak — Jupiter's overstatement admits 212 sub-$5.1K tokens that lose 3 of
4 times.

## 3. Entry-timing divergence — the 2-minute deferral the backtest doesn't model

The live loop holds every token in a watch list and refuses to screen anything
younger than `MIN_EVAL_AGE_S = 120` (MT-604). The backtest evaluates from the
first 5s bar and enters at the **first gate-passing bar** with no minimum age.

Simulating backtest-style first-pass entry on the same 1,110 mints live traded:

| entry rule | n | WR |
| --- | ---: | ---: |
| backtest first-pass entry (no min age) | 1,110 | **58.7%** |
| backtest first-pass entry, min age 60s | 1,022 | 50.6% |
| backtest first-pass entry, min age 120s | 865 | **49.1%** |

The backtest's median entry age is **0.29 min** (winners enter at ~4.4e-06 SOL vs
losers 3.3e-05 — a ~7.5x price difference); live's median entry age is **1.39 min**
(Jupiter-reported) — and since Jupiter's `created_timestamp` runs ~0.65 min
*later* than on-chain birth (§5), live's *real* entry age is ~2.0 min. The first
2 minutes contain most of the upside the backtest harvests.

This is a **structural divergence MT-607 did not catch**: the backtest has no
`MIN_EVAL_AGE_S` equivalent. On identical mints, allowing first-pass entry raises
WR from ~49% (120s-deferred) to 58.7% — **9.6 pts, ~72% of the per-mint gap**.

**Interaction with H1:** 91% of the 382 gate-rejected live entries are younger
than 2 min (Jupiter-reported). The data mismatch is worst exactly in the window
where the backtest buys — Jupiter's snapshot of a 0-2 min old token is the least
reliable, and that is precisely the entry window the backtest exploits.

## 4. H2 — Entry/exit price mismatch (minor)

- **Entry price:** live `entry_price_sol` vs parquet OHLCV close at the same
  timestamp: **median −0.23%** (live enters slightly *below* parquet close — no
  systematic overpay). Median vs next-bar open (the backtest's entry reference):
  −0.20%. The enormous mean (+16,022%) comes from parquet mcap artifacts
  (bt_mcap $2 / $870) — not representative.
- **Exit outcome agreement:** live win/lose vs the parquet-bar exit simulation
  agrees **84.8%** of the time. The 15.2% flips are nearly symmetric (84
  live_lose→sim_win vs 74 live_win→sim_lose) — net **+0.82 pts** toward the
  backtest (**12% of the same-day gap**). The dominant flips are
  trailing_stop↔hard_stop (63+61): the same losing trades sampled at 30s bar
  closes vs live's 30s Jupiter polls land on opposite sides of the −8% stop.
- **Hard-stop check:** of 513 live hard-stop losers, the parquet bars confirmed a
  ≥8% drawdown in the same window in the overwhelming majority of cases — the stop
  itself is not misfiring; the *entry selection* is the problem.

**Conclusion:** H2 is real but small — exit sampling flips ~15% of trades with
~zero net WR effect; entry price alignment is near-perfect at the median.

## 5. Jupiter age under-reporting (H1 subclass)

Live computes age from Jupiter `created_timestamp`; the parquet uses on-chain
birth. At the same wall-clock bar, Jupiter's age is **~0.65 min younger (median)**
than the parquet age — Jupiter reports tokens as younger than they are. Effects:
- Live's effective entry age is ~2.0 min, not the 1.39 min it logs.
- The age-adjusted txn threshold (`_age_adjusted_min_txns`) is evaluated against
  the under-stated age, slightly easing the txn gate.
- 617 entries logged live-age < 2 min while the parquet age was ≥ 2 min.

## 6. Aug 22 note

The enriched parquet for 2026-08-22 does not exist yet: the ETL only finalizes a
day after all 24 raw hours complete, and enrichment additionally requires a
SOL/USD price row. During this diagnostic the missing 2026-08-21 price was added
to `derived/sol_prices.csv` (CoinGecko, $94.18), which unblocked the Aug 21
enrichment (4,663,700 rows). Aug 22 entries (333) are therefore unmatchable until
the day finalizes; the divergence mechanism is day-agnostic, so the Aug 21 window
is the evidence base.

## 7. Categorized WR-gap attribution

**Same-day gap (6.64 pts, 51.98% backtest vs 45.34% live):**

| factor | WR effect | share |
| --- | ---: | ---: |
| H1 data mismatch (filtering the 382 would-be-rejected live entries) | +4.30 pts | ~65% |
| Entry timing / other residual (backtest buys 1.7 min earlier) | ~+1.5 pts | ~23% |
| H2 price/exit net flips (84 vs 74) | +0.82 pts | ~12% |

**Per-mint gap on identical mints (13.4 pts, 58.7% first-pass vs 45.34% live):**

| factor | WR effect | share |
| --- | ---: | ---: |
| Entry timing (first-pass vs 120s deferral) | +9.6 pts | ~72% |
| Gate-input data + price residual | +3.8 pts | ~28% |

The headline 23-point gap = same-day mechanism gap (6.64 pts) + **period mix**
(~17.8 pts: the 69.8% backtest number is a 122-day average, and Aug 21 was a
below-average day for the backtest too).

## 8. Recommended fixes (prioritized)

1. **Reconcile the mcap gate** — the highest-yield data fix. Options: (a) derive
   mcap from Jupiter `fdv`/supply×price instead of `mcap`, or (b) verify mcap
   against a second source before entry. Filtering the 212 mcap-below-floor
   cohort alone is worth ~+4.5 pts on live WR.
2. **Align entry timing with the backtest** — either lower `MIN_EVAL_AGE_S`
   toward the backtest's first-pass behavior (with the MT-604 false-positive
   caveat) or add the 120s deferral to the backtest so targets are honest. Until
   decided, the backtest overstates achievable WR by ~9.6 pts per token.
3. **Treat Jupiter mcap/age as advisory inputs** — they diverge from on-chain by
   −7% (mcap), −24% (txns), and −0.65 min (age); don't let ±10-30% snapshot
   noise flip a gate calibrated on on-chain data.
4. **Aug 22 backfill** — after the day finalizes, extend this diagnostic to the
   Aug 22 entries to confirm the same ratios.
