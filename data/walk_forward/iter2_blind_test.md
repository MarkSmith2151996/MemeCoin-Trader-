# Iteration 2 Blind Test — July 2026 (replay)

Blind window: **2026-07-01..2026-07-31**

Tuned gates applied with zero adjustment: `{"pool_sol_min": 3.572028, "creator_holdings_max": 0.0, "score_v1_min": 0.294855}`

## Baseline (replay, MT-569 friction)

- Trades: **74616**
- Win rate: **46.7%** (34847 wins / 39769 losses)
- Total PnL: **+205.419291 SOL**
- Avg PnL per trade: +0.002753 SOL

## Tuned gates (applied blind, no adjustment)

- Trades: **69253** (92.8% retention)
- Win rate: **48.9%** (33896 wins / 35357 losses)
- Total PnL: **+219.161841 SOL**
- Avg PnL per trade: +0.003165 SOL

## Comparison

| metric | baseline | tuned | delta |
|---|---:|---:|---:|
| trades | 74616 | 69253 | -5363 (93% retained) |
| win rate | 46.7% | 48.9% | +2.2pp |
| total PnL | +205.419291 SOL | +219.161841 SOL | +13.742550 SOL |
| avg PnL/trade | +0.002753 SOL | +0.003165 SOL | +0.000412 SOL |

## Verdict checks (all four must hold)

| check | result |
|---|---|
| total PnL >= baseline | PASS |
| win rate improved >= 2pp | PASS |
| retained >= 40% of trades | PASS |
| avg PnL/trade >= baseline | PASS |

## Verdict: **PASS**

### Removed cohort (baseline trades the tuned gates excluded)

Removed 5363 trades: WR 17.7%, PnL -13.742550 SOL, avg -0.002562 SOL/trade

Tier breakdown of the removed cohort:

  | mcap tier | trades | wins | win rate | PnL (SOL) |
  |---|---|---:|---:|---:|
  | $10-20K | 1449 | 246 | 17.0% | -3.8011 |
  | $5-10K | 2829 | 524 | 18.5% | -6.5637 |
  | $20-50K | 1085 | 181 | 16.7% | -3.3777 |

Exit breakdown of the removed cohort:

  | exit | count |
  |---|---:|
  | hard_stop | 4185 |
  | trailing | 637 |
  | tp | 488 |
  | time_stop | 53 |

### Tuned cohort exit breakdown

  | exit | count |
  |---|---:|
  | trailing | 48215 |
  | hard_stop | 19128 |
  | tp | 1240 |
  | time_stop | 670 |

