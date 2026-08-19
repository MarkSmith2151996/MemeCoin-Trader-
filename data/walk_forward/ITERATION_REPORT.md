# Walk-Forward Auto-Tuner — Iteration Report v1 (MT-591)

Date: 2026-08-19
Task: MT-591 — train week 1 (2026-08-05..08-11), blind-test week 2 (2026-08-12..08-18), replay-validate only if the blind test passes.

## Summary

| step | outcome |
|---|---|
| Week 1 training | 2 gates learned: `mcap_min_usd = 6,874`, `buy_sell_min = 0.81` |
| Week 2 blind test | **FAIL** — win rate improved +4.2pp but total PnL dropped −12.55 SOL |
| Replay validation | **NOT RUN** (blocked by blind-test failure, per task Step 4 gate) |
| Recommendation | **Iterate the tuner** — do not ship `tuned_gates_v1.json` |

## Step 1 — Training data

618 closed week-1 positions (35.6% WR, +3.149 SOL) joined to entry-time `candidate_log` rows. All entry features available except `dev_holdings_pct` (0% populated — dropped) and `liquidity_usd` (214/618 populated — tunable but heavily missing). A `score_proxy` replicates the MT-588 composite strength score from stored fields. Label: `realized_pnl_sol > 0`. Outputs: `data/walk_forward/week1_features.csv` (618 rows), `week2_features.csv` (1797 rows).

## Step 2 — What was learned

Method: per-gate threshold scan (10th–95th percentiles) maximizing passing-cohort total PnL, subject to cohort ≥ 50 trades and WR ≥ baseline − 5pp, then greedy forward selection (stop when a gate adds < 2% PnL, max 5 gates).

Per-gate best thresholds on train:

| gate | direction | threshold | trades | WR | PnL | lift |
|---|---:|---:|---:|---:|---:|---:|
| mcap_min_usd | min | 6,874 | 463 | 42.3% | +3.524 | 1.12x |
| top10_holder_max | max | 100.0 | 463 | 42.5% | +3.480 | 1.10x |
| buy_sell_min | min | 0.81 | 556 | 36.9% | +3.279 | 1.04x |
| volume_min_usd | min | 611 | 556 | 37.8% | +3.205 | 1.02x |
| score_min | min | 67.3 | 464 | 38.6% | +3.168 | 1.01x |

Forward selection picked **`mcap_min_usd = 6,873.78`** then **`buy_sell_min = 0.8097`**: 393/618 train trades retained, WR 44.8% (+9.2pp), PnL +3.612 (+0.463 SOL, +14.7%). Config saved to `data/walk_forward/tuned_gates_v1.json`. `top10_holder_max = 100.0` was not selected — it acts as a data-quality filter, not a signal.

## Step 3 — Blind test on week 2 (did it generalize? NO)

Applied the two tuned gates to all 1797 week-2 trades with zero adjustment:

| metric | actual | tuned | delta |
|---|---:|---:|---:|
| trades | 1,797 | 1,092 | −705 (60.8% retained) |
| win rate | 56.6% | 60.8% | **+4.2pp** |
| total PnL | +29.946 SOL | +17.399 SOL | **−12.547 SOL** |
| avg PnL/trade | +0.0167 SOL | +0.0159 SOL | −0.0007 SOL |

Verdict: **FAIL**. Win rate improved and the cohort stayed large, but total PnL fell 42% — the tuned gates cut 705 trades that were collectively +12.55 SOL at a 50.1% win rate. Full comparison in `data/walk_forward/blind_test_v1.md`.

### Root cause

Regime shift between weeks. Week 1 was a bad, low-mcap-dominated week: the <$5K tier ran 11.3% WR (−0.30 SOL) and nearly all PnL concentrated in the $20–50K tier (+3.37 SOL). Week 2 was a good week where profit spread across all tiers:

- Week 2 **$5–7K tier: 501 trades, 56.9% WR, +9.71 SOL** — the `mcap_min` threshold (6,874) sits inside that tier and cut most of it.
- Week 2 **0.5–0.8 buy/sell bucket: +3.28 SOL** — removed by the `buy_sell_min` gate.
- Removed cohort PnL was spread across the week (Aug 15: +2.88, Aug 17: +5.69, Aug 18: +2.37), i.e. not one lucky day — the filtering was structurally wrong for the week.

In short: the week-1 signal ("avoid low mcap / low buy-sell") **inverted** in week 2. The tuner fit a single week's regime with 618 samples, and the PnL objective is dominated by a handful of +0.075 take-profits, so the learned boundaries were noise-amplifying.

## Step 4 — Replay validation

**Not run.** Task Step 4 is gated on the blind test showing improvement; it did not. Baseline for a future run (MT-569): +7.451 SOL at 0.05 SOL / 3% slippage over 55 days (2026-04-18..06-11, `D:\pumpapi-replay\results\summary.csv` row 1). The tuned replay engine needs: (a) gates mapped into `replay_stratb.py` `passes_gates()` (mcap, cumulative volume, buy/sell, txns, age, vol/mcap, pool depth, top10/creator holdings, strength score), and (b) the MT-569 friction model (3% slippage + market impact on entry) so PnL is comparable — the stock `replay_stratb.py` models no friction.

## Step 5 — Recommendation and tuner adjustments for iteration 2

Do not ship `tuned_gates_v1.json`. Suggested changes, in priority order:

1. **More training data, spanning multiple regimes.** 618 trades from one bad week is too thin and regime-specific. Train on 3–4 weeks (e.g. 2026-07-21..08-11) and blind-test on 08-12..08-18; the tuner should only accept a threshold that generalizes across the train weeks' tier structure.
2. **Stability-first objective.** Require each candidate threshold to improve PnL on **both halves of the training window** (or each training week independently), not just the pooled week. This kills regime-fitting like the mcap gate.
3. **Per-tier or interaction-aware gating instead of global min-thresholds.** The failure came from hard-thresholding inside a dense, profitable bucket ($5–7K). Options: gate on mcap tiers with tier-specific buy/sell requirements, use a depth-2 decision tree with a minimum-cohort-per-leaf constraint, or add a "preserve ≥ X% of each tier" constraint.
4. **Weight the objective toward avg PnL/trade and profit factor** with a minimum cohort size scaled to the week (e.g. ≥ 15% of train), so one 0.075-SOL take-profit cluster cannot dominate the choice.
5. **Validate on replay before any live change.** Even a passing blind test is one week — the 4-month replay (122 days, 2026-04-18..08-17) remains the final arbiter, and the tuned-gates run must use the same friction model as the MT-569 baseline.

## Artifacts

- `data/walk_forward/week1_features.csv` — train features (618 rows)
- `data/walk_forward/week2_features.csv` — blind-test features (1797 rows)
- `data/walk_forward/tuned_gates_v1.json` — learned config + per-gate search table
- `data/walk_forward/blind_test_v1.md` — full blind-test comparison and verdict
- `scripts/walk_forward_extract.py`, `scripts/walk_forward_tune.py`, `scripts/walk_forward_blind_test.py` — reproducible pipeline (offline, read-only; the live loop was not touched)
