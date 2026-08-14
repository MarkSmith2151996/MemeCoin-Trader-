# Strategy B Analytics Report

_Generated: 2026-08-14 22:46:57 UTC — data from /home/dev/projects/memecoin-trader/data/trades.db_

## 1. Overall Summary
- Total closed trades: **762**
- Win rate: **36.7%** (280W / 416L / 66 flat)
- Total PnL: **+4.2139 SOL**
- Average PnL per trade: +0.0055 SOL
- Average win: +0.0336 SOL (n=280)
- Average loss: -0.0125 SOL (n=416)
- Profit factor: **1.810**
- Open positions: **0**

## 2. PnL by Mcap Tier
| Tier       | Trades   | PnL (SOL)    | Win rate   | Avg PnL    |
|------------|----------|--------------|------------|------------|
| <$5K       | 134      | -0.3182      | 10.4%      | -0.0024    |
| $5K-10K    | 108      | +0.4590      | 38.9%      | +0.0043    |
| $10K-20K   | 71       | +0.2254      | 38.0%      | +0.0032    |
| $20K-50K   | 449      | +3.8477      | 43.9%      | +0.0086    |
| $50K+      | 0        | +0.0000      | 0.0%       | +0.0000    |

> Note: `<$5K` tier is now blocked by gate config (manual_freeze on 2026-08-13, min_mcap_usd=5000).

## 3. Win Rate by Day of Week
| Day    | Trades   | PnL (SOL)    | Win rate   | Flag                 |
|--------|----------|--------------|------------|----------------------|
| Mon    | 0        | +0.0000      | —          |                      |
| Tue    | 54       | +0.3123      | 29.6%      |                      |
| Wed    | 94       | -0.8805      | 21.3%      | **<25%**             |
| Thu    | 435      | +3.4787      | 37.9%      |                      |
| Fri    | 179      | +1.3035      | 44.1%      |                      |
| Sat    | 0        | +0.0000      | —          |                      |
| Sun    | 0        | +0.0000      | —          |                      |


## 4. Win Rate by UTC Hour
| UTC hour   | Trades   | PnL (SOL)    | Win rate   | Blocked?   | Flag                 |
|------------|----------|--------------|------------|------------|----------------------|
| 0          | 0        | +0.0000      | —          | YES        |                      |
| 1          | 36       | +0.1690      | 33.3%      |            |                      |
| 2          | 56       | +0.3191      | 33.9%      |            |                      |
| 3          | 66       | +0.3790      | 37.9%      |            |                      |
| 4          | 43       | +0.3327      | 39.5%      |            |                      |
| 5          | 54       | +0.5815      | 51.9%      |            |                      |
| 6          | 37       | +0.3624      | 54.1%      |            |                      |
| 7          | 0        | +0.0000      | —          | YES        |                      |
| 8          | 10       | +0.1481      | 50.0%      |            |                      |
| 9          | 11       | +0.0888      | 45.5%      |            |                      |
| 10         | 13       | +0.1460      | 53.8%      |            |                      |
| 11         | 9        | +0.0954      | 44.4%      |            |                      |
| 12         | 17       | +0.4320      | 64.7%      |            |                      |
| 13         | 23       | +0.2289      | 34.8%      |            |                      |
| 14         | 25       | -0.1287      | 16.0%      |            | **<25% unblocked**   |
| 15         | 50       | +0.3090      | 32.0%      |            |                      |
| 16         | 41       | +0.2240      | 29.3%      |            |                      |
| 17         | 46       | +0.4742      | 47.8%      |            |                      |
| 18         | 36       | +0.2342      | 33.3%      |            |                      |
| 19         | 4        | -0.0051      | 25.0%      | YES        |                      |
| 20         | 24       | -0.4847      | 20.8%      | YES        |                      |
| 21         | 49       | -0.3465      | 20.4%      | YES        |                      |
| 22         | 66       | +0.4523      | 36.4%      |            |                      |
| 23         | 46       | +0.2023      | 28.3%      |            |                      |

> Blocked UTC hours: [0, 7, 19, 20, 21]

## 5. Exit Type Breakdown
| Exit type          | Trades   | PnL (SOL)    | Avg PnL    | Win rate   |
|--------------------|----------|--------------|------------|------------|
| hard_stop          | 318      | -4.7624      | -0.0150    | 0.0%       |
| take_profit        | 163      | +7.8400      | +0.0481    | 100.0%     |
| early_exit_no_green | 127      | -0.2744      | -0.0022    | 4.7%       |
| time_stop          | 108      | +1.1359      | +0.0105    | 73.1%      |
| trailing_stop      | 46       | +0.2748      | +0.0060    | 69.6%      |


## 6. Daily PnL Timeline
| Date         | Trades   | PnL (SOL)    | Cumulative   |
|--------------|----------|--------------|--------------|
| 2026-08-05   | 90       | -0.8738      | -0.8738      |
| 2026-08-06   | 393      | +3.1053      | +2.2314      |
| 2026-08-07   | 80       | +0.5656      | +2.7970      |
| 2026-08-11   | 55       | +0.3523      | +3.1493      |
| 2026-08-12   | 2        | -0.0011      | +3.1482      |
| 2026-08-13   | 42       | +0.3387      | +3.4869      |
| 2026-08-14   | 100      | +0.7271      | +4.2139      |

- Best day: **2026-08-06** (+3.1053 SOL, 393 trades)
- Worst day: **2026-08-05** (-0.8738 SOL, 90 trades)

## 7. Slippage-Adjusted PnL by Tier
| Tier       | Slippage   | Trades   | Paper PnL    | Slip cost    | Realistic PnL  |
|------------|------------|----------|--------------|--------------|----------------|
| <$5K       | 8%         | 134      | -0.3182      | -1.0465      | -1.3648        |
| $5K-10K    | 8%         | 108      | +0.4590      | -0.9407      | -0.4817        |
| $10K-20K   | 5%         | 71       | +0.2254      | -0.3663      | -0.1408        |
| $20K-50K   | 3%         | 449      | +3.8477      | -1.4624      | +2.3853        |
| $50K+      | 3%         | 0        | +0.0000      | -0.0000      | +0.0000        |

> Slippage model: round-trip cost = entry slippage (amount_sol × pct) + exit slippage (exit value × pct); `<$10K`=8%, `$10-20K`=5%, `$20K+`=3%.

## 8. Gate Effectiveness
### Gate #1 — 2026-08-05 18:36 UTC — INITIAL
- Config: `max_age_minutes=30, min_buy_sell_ratio=0.4, min_mcap_usd=2000, min_volume_usd=200`
- Cohort: 0 trades, PnL +0.0000 SOL, win rate —

### Gate #2 — 2026-08-05 22:08 UTC — auto_tuned
- Config: `max_age_minutes=22.5, min_buy_sell_ratio=0.5, min_mcap_usd=2500.0, min_volume_usd=250.0`
- Cohort: 54 trades, PnL -0.8357 SOL, win rate 18.5%

### Gate #3 — 2026-08-06 01:38 UTC — auto_tuned
- Config: `max_age_minutes=22.5, min_buy_sell_ratio=0.5, min_mcap_usd=2500.0, min_volume_usd=250.0`
- Cohort: 46 trades, PnL -0.0002 SOL, win rate 26.1%

### Gate #4 — 2026-08-06 06:10 UTC — auto_tuned
- Config: `max_age_minutes=16.875, min_buy_sell_ratio=0.625, min_mcap_usd=3125.0, min_volume_usd=312.5`
- Cohort: 51 trades, PnL +0.4687 SOL, win rate 39.2%

### Gate #5 — 2026-08-06 11:29 UTC — auto_tuned
- Config: `max_age_minutes=12.65625, min_buy_sell_ratio=0.78125, min_mcap_usd=3906.25, min_volume_usd=390.625`
- Cohort: 51 trades, PnL +0.4009 SOL, win rate 47.1%

### Gate #6 — 2026-08-06 14:04 UTC — auto_tuned
- Config: `max_age_minutes=22.5, min_buy_sell_ratio=0.5, min_mcap_usd=2500.0, min_volume_usd=250.0`
- Cohort: 49 trades, PnL +0.6716 SOL, win rate 42.9%

### Gate #7 — 2026-08-06 15:46 UTC — auto_tuned
- Config: `max_age_minutes=16.875, min_buy_sell_ratio=0.625, min_mcap_usd=3125.0, min_volume_usd=312.5`
- Cohort: 49 trades, PnL +0.1004 SOL, win rate 28.6%

### Gate #8 — 2026-08-06 17:50 UTC — auto_tuned
- Config: `max_age_minutes=12.65625, min_buy_sell_ratio=0.78125, min_mcap_usd=3906.25, min_volume_usd=390.625`
- Cohort: 52 trades, PnL +0.4868 SOL, win rate 44.2%

### Gate #9 — 2026-08-06 21:17 UTC — auto_tuned
- Config: `max_age_minutes=22.5, min_buy_sell_ratio=0.5, min_mcap_usd=2500.0, min_volume_usd=250.0`
- Cohort: 49 trades, PnL +0.1853 SOL, win rate 26.5%

### Gate #10 — 2026-08-06 22:51 UTC — auto_tuned
- Config: `max_age_minutes=16.875, min_buy_sell_ratio=0.625, min_mcap_usd=3125.0, min_volume_usd=312.5`
- Cohort: 49 trades, PnL +0.5056 SOL, win rate 42.9%

### Gate #11 — 2026-08-07 01:31 UTC — auto_tuned
- Config: `max_age_minutes=12.65625, min_buy_sell_ratio=0.78125, min_mcap_usd=3906.25, min_volume_usd=390.625`
- Cohort: 51 trades, PnL +0.3186 SOL, win rate 33.3%

### Gate #12 — 2026-08-07 05:41 UTC — auto_tuned
- Config: `max_age_minutes=9.492188, min_buy_sell_ratio=0.976562, min_mcap_usd=4882.8125, min_volume_usd=488.28125`
- Cohort: 50 trades, PnL +0.2647 SOL, win rate 40.0%

### Gate #13 — 2026-08-11 16:45 UTC — auto_tuned
- Config: `max_age_minutes=15.0, min_buy_sell_ratio=0.5, min_mcap_usd=1250.0, min_volume_usd=625.0`
- Cohort: 50 trades, PnL +0.4315 SOL, win rate 34.0%

### Gate #14 — 2026-08-13 03:59 UTC — auto_tuned
- Config: `max_age_minutes=15.0, min_buy_sell_ratio=0.5, min_mcap_usd=1250.0, min_volume_usd=625.0`
- Cohort: 49 trades, PnL +0.4312 SOL, win rate 38.8%

### Gate #15 — 2026-08-13 18:15 UTC — manual_freeze
- Config: `max_age_minutes=22, min_buy_sell_ratio=0.5, min_mcap_usd=5000, min_volume_usd=500`
- Cohort: 12 trades, PnL +0.0574 SOL, win rate 33.3%

### After last gate #15 (2026-08-13 18:15 UTC)
- Cohort: 100 trades, PnL +0.7271 SOL, win rate 45.0%

## 9. Shadow Mode Data (Jupiter Quotes)
- No rows in `jupiter_quotes` — shadow mode is not capturing data yet (0 quotes).
