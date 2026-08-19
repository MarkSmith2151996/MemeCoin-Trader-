# Iteration 1 Blind Test — June 2026 (replay)

Blind window: **2026-06-01..2026-06-30**

Tuned gates applied with zero adjustment: `{"score_v1_min": 2.203429, "age_max_minutes": 8.5088, "pool_sol_min": 14.275071, "vol_mcap_max": 2.581956, "buy_sell_min": 1.383184}`

## Baseline (replay, MT-569 friction)

- Trades: **64373**
- Win rate: **43.0%** (27690 wins / 36683 losses)
- Total PnL: **+224.567433 SOL**
- Avg PnL per trade: +0.003489 SOL

## Tuned gates (applied blind, no adjustment)

- Trades: **16778** (26.1% retention)
- Win rate: **51.9%** (8706 wins / 8072 losses)
- Total PnL: **+104.271980 SOL**
- Avg PnL per trade: +0.006215 SOL

## Comparison

| metric | baseline | tuned | delta |
|---|---:|---:|---:|
| trades | 64373 | 16778 | -47595 (26% retained) |
| win rate | 43.0% | 51.9% | +8.9pp |
| total PnL | +224.567433 SOL | +104.271980 SOL | -120.295453 SOL |
| avg PnL/trade | +0.003489 SOL | +0.006215 SOL | +0.002726 SOL |

## Verdict checks (all four must hold)

| check | result |
|---|---|
| total PnL >= baseline | FAIL |
| win rate improved >= 2pp | PASS |
| retained >= 40% of trades | FAIL |
| avg PnL/trade >= baseline | PASS |

## Verdict: **FAIL**

### Removed cohort (baseline trades the tuned gates excluded)

Removed 47595 trades: WR 39.9%, PnL +120.295453 SOL, avg +0.002527 SOL/trade

Tier breakdown of the removed cohort:

  | mcap tier | trades | wins | win rate | PnL (SOL) |
  |---|---|---:|---:|---:|
  | $20-50K | 12970 | 5205 | 40.1% | +45.5608 |
  | $5-10K | 13461 | 4823 | 35.8% | +26.9954 |
  | $10-20K | 21164 | 8956 | 42.3% | +47.7393 |

Exit breakdown of the removed cohort:

  | exit | count |
  |---|---:|
  | trailing | 30939 |
  | hard_stop | 14170 |
  | tp | 1603 |
  | time_stop | 883 |

### Tuned cohort exit breakdown

  | exit | count |
  |---|---:|
  | trailing | 11326 |
  | hard_stop | 4444 |
  | tp | 576 |
  | time_stop | 432 |

