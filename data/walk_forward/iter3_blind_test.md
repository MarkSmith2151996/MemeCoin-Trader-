# Iteration 3 Blind Test — Aug 1-17 2026 (replay)

Blind window: **2026-08-01..2026-08-17**

Tuned gates applied with zero adjustment: `{"pool_sol_min": 4.466964, "creator_holdings_max": 0.0, "mcap_min_usd": 5117.29257}`

## Baseline (replay, MT-569 friction)

- Trades: **49616**
- Win rate: **39.4%** (19564 wins / 30052 losses)
- Total PnL: **+228.904660 SOL**
- Avg PnL per trade: +0.004614 SOL

## Tuned gates (applied blind, no adjustment)

- Trades: **44371** (89.4% retention)
- Win rate: **42.1%** (18686 wins / 25685 losses)
- Total PnL: **+241.863079 SOL**
- Avg PnL per trade: +0.005451 SOL

## Comparison

| metric | baseline | tuned | delta |
|---|---:|---:|---:|
| trades | 49616 | 44371 | -5245 (89% retained) |
| win rate | 39.4% | 42.1% | +2.7pp |
| total PnL | +228.904660 SOL | +241.863079 SOL | +12.958419 SOL |
| avg PnL/trade | +0.004614 SOL | +0.005451 SOL | +0.000837 SOL |

## Verdict checks (all four must hold)

| check | result |
|---|---|
| total PnL >= baseline | PASS |
| win rate improved >= 2pp | PASS |
| retained >= 40% of trades | PASS |
| avg PnL/trade >= baseline | PASS |

## Verdict: **PASS**

### Removed cohort (baseline trades the tuned gates excluded)

Removed 5245 trades: WR 16.7%, PnL -12.958419 SOL, avg -0.002471 SOL/trade

Tier breakdown of the removed cohort:

  | mcap tier | trades | wins | win rate | PnL (SOL) |
  |---|---|---:|---:|---:|
  | $10-20K | 1041 | 146 | 14.0% | -3.2708 |
  | $5-10K | 3286 | 567 | 17.3% | -7.3628 |
  | $20-50K | 918 | 165 | 18.0% | -2.3248 |

Exit breakdown of the removed cohort:

  | exit | count |
  |---|---:|
  | hard_stop | 4183 |
  | trailing | 592 |
  | tp | 463 |
  | time_stop | 7 |

### Tuned cohort exit breakdown

  | exit | count |
  |---|---:|
  | trailing | 20923 |
  | hard_stop | 20792 |
  | tp | 2133 |
  | time_stop | 523 |

