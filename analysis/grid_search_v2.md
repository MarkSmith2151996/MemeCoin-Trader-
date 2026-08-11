# MT-520 Grid Search V2

## Method

Four chronological overlapping walk-forward folds use a 50% training and 25% test slice.
The ranking uses test-fold PnL; a selected winner must beat its current baseline on at least 3 of 4 folds.
Price-linked snapshots are replayed in timestamp order when available; older positions use the MT-502 peak-bound fallback.

## Current Settings

```json
{
  "A": {
    "max_top10_holder_pct": 80,
    "trailing_stop_pct": 4,
    "take_profit_pct": 60,
    "hard_stop_pct": 10,
    "early_exit_timeout_s": 90,
    "early_exit_threshold_pct": 1,
    "repeat_loser_cooldown_hours": 2,
    "max_concurrent_positions": 4
  },
  "B": {
    "max_age_minutes": 30,
    "min_mcap_usd": 2000,
    "min_volume_usd": 200,
    "min_txns": 3,
    "trailing_stop_pct": null,
    "take_profit_pct": 100,
    "hard_stop_pct": 30,
    "early_exit_timeout_s": 90,
    "early_exit_threshold_pct": 1
  }
}
```

## Strategy A

Closed positions analyzed: 1815
Exit confidence: **HIGH**. Majority-window winner: **True**.

### Top 5 Exit Combinations

| Rank | Trailing | TP | Hard stop | Early timeout | Early threshold | Mean test PnL | Window wins | Win rate | Sharpe | Max drawdown |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 3% | 60% | 8% | 90s | 2% | +1.21144 | 4/4 | 27.4% | 4.65 | -0.19694 |
| 2 | 3% | 60% | 8% | 90s | 3% | +1.21144 | 4/4 | 27.4% | 4.65 | -0.19694 |
| 3 | 3% | 60% | 8% | 90s | 1% | +1.21120 | 4/4 | 27.4% | 4.65 | -0.19694 |
| 4 | 3% | 60% | 8% | 60s | 3% | +1.21117 | 4/4 | 27.4% | 4.65 | -0.19694 |
| 5 | 3% | 60% | 8% | 60s | 2% | +1.21106 | 4/4 | 27.4% | 4.65 | -0.19694 |

### Sensitivity

The winner's +/-1% trailing and +/-10% TP neighborhood has **100.0%** positive combinations.

### Entry Controls

Strategy A does not persist holder concentration or rejected candidate outcomes. `MAX_TOP10_HOLDER_PCT`, cooldown, and concurrency therefore remain at their current conservative values (80%, 2h, 4). Changing them from this dataset would be unsupported.


## Strategy B

Closed positions analyzed: 576
Exit confidence: **HIGH**. Majority-window winner: **True**.

### Top 5 Exit Combinations

| Rank | Trailing | TP | Hard stop | Early timeout | Early threshold | Mean test PnL | Window wins | Win rate | Sharpe | Max drawdown |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 4% | 80% | 10% | 90s | 1% | +1.05305 | 4/4 | 43.1% | 9.46 | -0.04568 |
| 2 | 5% | 80% | 10% | 90s | 1% | +1.05257 | 4/4 | 43.1% | 9.46 | -0.04568 |
| 3 | 5% | 80% | 10% | 60s | 1% | +1.05253 | 4/4 | 43.1% | 9.46 | -0.04568 |
| 4 | 4% | 80% | 10% | 60s | 1% | +1.05200 | 4/4 | 43.1% | 9.46 | -0.04568 |
| 5 | 4% | 80% | 10% | 60s | 3% | +1.05137 | 4/4 | 43.1% | 9.46 | -0.04568 |

### Sensitivity

The winner's +/-1% trailing and +/-10% TP neighborhood has **100.0%** positive combinations.

### Gate Width Search

Rejected candidates have no observed later price path, so widening gates cannot be counterfactually scored.
Linked entered outcomes: 576; valid combinations: 320.

| Rank | Max age | Min mcap | Min volume | Min txns | Trades | Mean test PnL | Window wins | Win rate | Sharpe | Max drawdown |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 20m | $1,000 | $500 | 3 | 541 | +1.05564 | 4/4 | 40.3% | 9.95 | -0.13909 |
| 2 | 20m | $2,000 | $500 | 3 | 541 | +1.05564 | 4/4 | 40.3% | 9.95 | -0.13909 |
| 3 | 30m | $1,000 | $500 | 3 | 547 | +1.05564 | 4/4 | 40.2% | 10.01 | -0.13909 |
| 4 | 30m | $2,000 | $500 | 3 | 547 | +1.05564 | 4/4 | 40.2% | 10.01 | -0.13909 |
| 5 | 20m | $1,000 | $100 | 3 | 570 | +1.05305 | 4/4 | 38.8% | 9.78 | -0.14814 |

## Recommendation

{
  "A": {
    "exit_parameters": {
      "trailing_stop_pct": 3,
      "take_profit_pct": 60,
      "hard_stop_pct": 8,
      "early_exit_timeout_s": 90,
      "early_exit_threshold_pct": 2
    },
    "entry_controls": "Keep holder=80%, cooldown=2h, concurrent=4: no Strategy A entry-outcome evidence."
  },
  "B": {
    "exit_parameters": {
      "trailing_stop_pct": 4,
      "take_profit_pct": 80,
      "hard_stop_pct": 10,
      "early_exit_timeout_s": 90,
      "early_exit_threshold_pct": 1
    },
    "gate_parameters": {
      "max_age_minutes": 20,
      "min_mcap_usd": 1000,
      "min_volume_usd": 500,
      "min_txns": 3
    }
  }
}

Confidence is limited by peak-bound fallback for historical paths and by the absence of outcomes for rejected candidates. The result is an evidence-based baseline, not proof that widened gates would have won.
