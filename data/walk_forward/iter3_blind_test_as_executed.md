# Iteration 3 Blind Test — Aug 1-17 2026 (replay)

Blind window: **2026-08-01..2026-08-17**

Tuned gates applied with zero adjustment: `{"pool_sol_min": 9.81695, "creator_holdings_max": 0.0}`

## Baseline (replay, MT-569 friction)

- Trades: **49616**
- Win rate: **39.4%** (19564 wins / 30052 losses)
- Total PnL: **+228.904660 SOL**
- Avg PnL per trade: +0.004614 SOL

## Tuned gates (applied blind, no adjustment)

- Trades: **41179** (83.0% retention)
- Win rate: **42.0%** (17277 wins / 23902 losses)
- Total PnL: **+215.435200 SOL**
- Avg PnL per trade: +0.005232 SOL

## Comparison

| metric | baseline | tuned | delta |
|---|---:|---:|---:|
| trades | 49616 | 41179 | -8437 (83% retained) |
| win rate | 39.4% | 42.0% | +2.5pp |
| total PnL | +228.904660 SOL | +215.435200 SOL | -13.469460 SOL |
| avg PnL/trade | +0.004614 SOL | +0.005232 SOL | +0.000618 SOL |

## Verdict checks (all four must hold)

| check | result |
|---|---|
| total PnL >= baseline | FAIL |
| win rate improved >= 2pp | PASS |
| retained >= 40% of trades | PASS |
| avg PnL/trade >= baseline | PASS |

## Verdict: **FAIL**

### Removed cohort (baseline trades the tuned gates excluded)

Removed 8437 trades: WR 27.1%, PnL +13.469460 SOL, avg +0.001596 SOL/trade

Tier breakdown of the removed cohort:

  | mcap tier | trades | wins | win rate | PnL (SOL) |
  |---|---|---:|---:|---:|
  | $20-50K | 3648 | 1284 | 35.2% | +16.3250 |
  | $10-20K | 2163 | 682 | 31.5% | +5.8552 |
  | $5-10K | 2626 | 321 | 12.2% | -8.7108 |

Exit breakdown of the removed cohort:

  | exit | count |
  |---|---:|
  | hard_stop | 5793 |
  | trailing | 1807 |
  | tp | 773 |
  | time_stop | 64 |

### Tuned cohort exit breakdown

  | exit | count |
  |---|---:|
  | trailing | 19708 |
  | hard_stop | 19182 |
  | tp | 1823 |
  | time_stop | 466 |

