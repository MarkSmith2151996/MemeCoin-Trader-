# Strategy B Sanity Sweep

Generated: 2026-08-15 17:10:57 UTC
Database: `/home/dev/projects/memecoin-trader/data/trades.db` (read-only)

## 1. Entry Filter Replay
Current gates: mcap >= $5,000; blocked UTC hours 0, 7, 19, 20, 21; Wednesday blocked.
| Cohort | Trades | PnL (SOL) | Win rate |
| --- | --- | --- | --- |
| All closed Strategy B | 1525 | +12.5109 SOL | 47.7% |
| Candidate-log linked | 1525 | +12.5109 SOL | 47.7% |
| Would survive current gates | 1318 | +13.4117 SOL | 52.4% |

- Missing candidate-log mcap: 0 trades (excluded from survivor cohort).

## 2. Exit Parameter Replay
Replay uses ordered position snapshots with the current 2% trail (armed at +2%), 150% TP, and 8% hard stop. A non-triggering path uses its recorded close as a terminal fallback.
| Replayable survivors | Actual PnL | Replayed PnL | Difference |
| --- | --- | --- | --- |
| 926 | +9.5601 SOL | +9.9322 SOL | +0.3721 SOL |

- Replay exits: hard_stop=344, recorded_close_fallback=88, take_profit=162, trailing_stop=332

## 3. Slippage Stress Test
Round-trip slippage is split equally between entry and exit value.
| Round-trip slippage | Trades | PnL (SOL) | Win rate |
| --- | --- | --- | --- |
| 0.5% | 926 | +9.7671 SOL | 55.3% |
| 1.0% | 926 | +9.6020 SOL | 55.1% |
| 2.0% | 926 | +9.2718 SOL | 54.2% |
| 3.0% | 926 | +8.9417 SOL | 53.3% |
| 5.0% | 926 | +8.2814 SOL | 51.6% |

## 4. Integrity Checks
| Metric | Value |
| --- | --- |
| Scan-to-entry rows | 1525 |
| Average delay | 10.2s |
| Minimum delay | 0.2s |
| Maximum delay | 1.3m |
| Delay under 2s | 60.1% |
| Negative delays (look-ahead) | 0 |

Repeated actual PnL values (rounded to 8 decimals; three or more occurrences):
| PnL (SOL) | Trades | Share of all closed trades |
| --- | --- | --- |
| -0.0020 SOL | 269 | 17.6% |
| -0.0150 SOL | 221 | 14.5% |
| +0.0375 SOL | 156 | 10.2% |
| +0.0500 SOL | 132 | 8.7% |
| -0.0050 SOL | 77 | 5.0% |
| +0.0000 SOL | 75 | 4.9% |
| +0.0400 SOL | 44 | 2.9% |
| -0.0024 SOL | 4 | 0.3% |
| -0.0500 SOL | 3 | 0.2% |

## 5. Drawdown Analysis
| Metric | Value |
| --- | --- |
| Max drawdown | -0.9444 SOL |
| Peak before drawdown | 2026-08-05T19:48:59.890712+00:00 |
| Drawdown trough | 2026-08-06T01:12:36.171819+00:00 |
| Time to recover | 619.8m |
| Worst losing streak | 9 trades / -0.1168 SOL |

## 6. Weekly PnL
| ISO week | Trades | PnL (SOL) | Flag |
| --- | --- | --- | --- |
| 2026-W32 | 563 | +2.7970 SOL |  |
| 2026-W33 | 962 | +9.7139 SOL |  |

## 7. Winner Concentration
| Scenario | PnL (SOL) | Profitable? |
| --- | --- | --- |
| All closed trades | +12.5109 SOL | YES |
| Remove top 5 winners | +12.2609 SOL | YES |
| Remove top 10 winners | +12.0109 SOL | YES |

- Top 10% of trades (153 rows) contribute +7.4673 SOL (59.7% of total PnL).

## Scope
- This sweep is read-only; it does not change Strategy B runtime logic, the live adapter, safety controls, or shadow-mode code.
