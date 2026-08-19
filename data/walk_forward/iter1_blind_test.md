# Iteration 1 Blind Test — June 2026 (replay)

Blind window: **2026-06-01..2026-06-30**

Tuned gates applied with zero adjustment: `{"pool_sol_min": 4.659425, "creator_holdings_max": 0.0, "mcap_min_usd": 5104.824172}`

## Baseline (replay, MT-569 friction)

- Trades: **64373**
- Win rate: **43.0%** (27690 wins / 36683 losses)
- Total PnL: **+224.567433 SOL**
- Avg PnL per trade: +0.003489 SOL

## Tuned gates (applied blind, no adjustment)

- Trades: **58979** (91.6% retention)
- Win rate: **45.4%** (26779 wins / 32200 losses)
- Total PnL: **+235.210150 SOL**
- Avg PnL per trade: +0.003988 SOL

## Comparison

| metric | baseline | tuned | delta |
|---|---:|---:|---:|
| trades | 64373 | 58979 | -5394 (92% retained) |
| win rate | 43.0% | 45.4% | +2.4pp |
| total PnL | +224.567433 SOL | +235.210150 SOL | +10.642717 SOL |
| avg PnL/trade | +0.003489 SOL | +0.003988 SOL | +0.000499 SOL |

## Verdict checks (all four must hold)

| check | result |
|---|---|
| total PnL >= baseline | PASS |
| win rate improved >= 2pp | PASS |
| retained >= 40% of trades | PASS |
| avg PnL/trade >= baseline | PASS |

## Verdict: **PASS**

### Removed cohort (baseline trades the tuned gates excluded)

Removed 5394 trades: WR 16.9%, PnL -10.642717 SOL, avg -0.001973 SOL/trade

Tier breakdown of the removed cohort:

  | mcap tier | trades | wins | win rate | PnL (SOL) |
  |---|---|---:|---:|---:|
  | $10-20K | 1439 | 230 | 16.0% | -2.9552 |
  | $5-10K | 2941 | 519 | 17.6% | -4.1050 |
  | $20-50K | 1014 | 162 | 16.0% | -3.5825 |

Exit breakdown of the removed cohort:

  | exit | count |
  |---|---:|
  | hard_stop | 4312 |
  | tp | 558 |
  | trailing | 506 |
  | time_stop | 18 |

### Tuned cohort exit breakdown

  | exit | count |
  |---|---:|
  | trailing | 41759 |
  | hard_stop | 14302 |
  | tp | 1621 |
  | time_stop | 1297 |

