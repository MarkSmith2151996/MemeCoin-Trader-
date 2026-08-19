# Iteration 2 Blind Test — July 2026 (replay)

Blind window: **2026-07-01..2026-07-31**

Tuned gates applied with zero adjustment: `{"score_v1_min": 3.026756}`

## Baseline (replay, MT-569 friction)

- Trades: **74616**
- Win rate: **46.7%** (34847 wins / 39769 losses)
- Total PnL: **+205.419291 SOL**
- Avg PnL per trade: +0.002753 SOL

## Tuned gates (applied blind, no adjustment)

- Trades: **63683** (85.3% retention)
- Win rate: **49.5%** (31500 wins / 32183 losses)
- Total PnL: **+200.635122 SOL**
- Avg PnL per trade: +0.003151 SOL

## Comparison

| metric | baseline | tuned | delta |
|---|---:|---:|---:|
| trades | 74616 | 63683 | -10933 (85% retained) |
| win rate | 46.7% | 49.5% | +2.8pp |
| total PnL | +205.419291 SOL | +200.635122 SOL | -4.784169 SOL |
| avg PnL/trade | +0.002753 SOL | +0.003151 SOL | +0.000398 SOL |

## Verdict checks (all four must hold)

| check | result |
|---|---|
| total PnL >= baseline | FAIL |
| win rate improved >= 2pp | PASS |
| retained >= 40% of trades | PASS |
| avg PnL/trade >= baseline | PASS |

## Verdict: **FAIL**

### Removed cohort (baseline trades the tuned gates excluded)

Removed 10933 trades: WR 30.6%, PnL +4.784169 SOL, avg +0.000438 SOL/trade

Tier breakdown of the removed cohort:

  | mcap tier | trades | wins | win rate | PnL (SOL) |
  |---|---|---:|---:|---:|
  | $10-20K | 2999 | 919 | 30.6% | +0.2906 |
  | $5-10K | 6266 | 1986 | 31.7% | +2.4684 |
  | $20-50K | 1668 | 442 | 26.5% | +2.0252 |

Exit breakdown of the removed cohort:

  | exit | count |
  |---|---:|
  | hard_stop | 6355 |
  | trailing | 3834 |
  | tp | 540 |
  | time_stop | 204 |

### Tuned cohort exit breakdown

  | exit | count |
  |---|---:|
  | trailing | 45018 |
  | hard_stop | 16958 |
  | tp | 1188 |
  | time_stop | 519 |

