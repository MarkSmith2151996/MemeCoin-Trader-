# Blind Test v1 — Week 2 (2026-08-12..2026-08-18)

Tuned gates from week 1: `{"mcap_min_usd": 6873.775000000001, "buy_sell_min": 0.8096906116642959}`

## Actual (hand-tuned gates, what the loop traded)

- Trades: **1797**
- Win rate: **56.6%** (1017 wins / 780 losses)
- Total PnL: **+29.945837 SOL**
- Avg PnL per trade: +0.016664 SOL

## Tuned gates (applied blind, no adjustment)

- Trades: **1092** (60.8% retention)
- Win rate: **60.8%** (664 wins / 428 losses)
- Total PnL: **+17.398746 SOL**
- Avg PnL per trade: +0.015933 SOL

## Comparison

| metric | actual | tuned | delta |
|---|---:|---:|---:|
| trades | 1797 | 1092 | -705 (61% retained) |
| win rate | 56.6% | 60.8% | +4.2pp |
| total PnL | +29.945837 SOL | +17.398746 SOL | -12.547091 SOL |
| avg PnL/trade | +0.016664 SOL | +0.015933 SOL | -0.000731 SOL |

## Verdict checks

| check | result |
|---|---|
| win rate improved | PASS |
| total PnL improved | FAIL |
| retained >= 40% of trades | PASS |

## Verdict: **FAIL — do not proceed to replay; iterate tuner**

### Notes

- Week 2 was a structurally different week than week 1 (56.6% vs 35.6% win rate at baseline), so a tuned-gate PnL beat over the actual results is a strong generalization signal.
- The tuned gates remove only the weakest tail of the funnel; retention is reported to judge the volume-vs-quality tradeoff.

### Tuned-gate cohort exit breakdown

  | exit | count |
  |---|---:|
  | hard_stop | 381 |
  | trailing_stop | 354 |
  | take_profit | 260 |
  | time_stop | 78 |
  | early_exit_no_green | 18 |

