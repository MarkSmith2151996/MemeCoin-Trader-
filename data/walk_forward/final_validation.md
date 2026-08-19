# Final Validation — Iteration 3 Gates vs Paper Trading (2026-08-05..2026-08-18)

> Paper trading data (CLOSED positions in `data/trades.db`, strategy B) was held out through all three tuning iterations and is touched only here.

Tuned gates: `{"pool_sol_min": 4.466964, "creator_holdings_max": 0.0, "mcap_min_usd": 5117.29257}`

## Gate mapping (replay -> paper)

| gate | applied? | paper feature |
|---|---|---|
| pool_sol_min >= 4.467 | yes | pool_sol_est |
| creator_holdings_max | no — creator_holdings_pct has no paper analog (dev_holdings_pct is 0% populated in this era) | — |
| mcap_min_usd >= 5,117.293 | yes | mcap_usd |

## Actual paper results (what the loop traded)

- Trades: **2415**
- Win rate: **51.2%** (1237 wins / 1178 losses)
- Total PnL: **+33.095160 SOL**
- Avg PnL per trade: +0.013704 SOL

## Tuned gates (applied to paper, no adjustment)

- Trades: **1630** (67.5% retention)
- Win rate: **59.0%** (962 wins / 668 losses)
- Total PnL: **+26.962836 SOL**
- Avg PnL per trade: +0.016542 SOL

## Comparison

| metric | actual | tuned | delta |
|---|---:|---:|---:|
| trades | 2415 | 1630 | -785 (67% retained) |
| win rate | 51.2% | 59.0% | +7.8pp |
| total PnL | +33.095160 SOL | +26.962836 SOL | -6.132324 SOL |
| avg PnL/trade | +0.013704 SOL | +0.016542 SOL | +0.002838 SOL |

## Verdict checks (all four must hold)

| check | result |
|---|---|
| total PnL >= baseline | FAIL |
| win rate improved >= 2pp | PASS |
| retained >= 40% of trades | PASS |
| avg PnL/trade >= baseline | PASS |

## Verdict: **FAIL — tuned gates do not beat actual paper results on the holdout**

### Removed cohort (paper trades the tuned gates would have excluded)

Removed 785 trades: WR 35.0%, PnL +6.132324 SOL, avg +0.007812 SOL/trade

### Caveats

- Paper trades were taken by the live loop with its own gates and at varying sizes/dates; the tuned cohort is a post-hoc subset comparison, not a re-execution.
- Gates skipped above were not enforceable on paper-era data (see mapping table).

