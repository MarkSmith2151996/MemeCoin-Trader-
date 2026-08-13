# Strategy B Entry Quality Analysis (MT-531)

**Snapshot:** 2026-08-13 UTC. Read-only analysis of `data/trades.db`: 633 closed Strategy B positions, all linked to an entered Strategy B `candidate_log` row. One Strategy B position was still open and is excluded. Candidate-log coverage was 14,310 candidates, 634 entries (4.43% entry rate).

## Executive Findings

- Strategy B is positive overall: 226/633 winners (35.7%), +3.329038 SOL total, +0.005259 SOL/trade. The win rate alone understates the result because take-profit winners are substantially larger than hard-stop losses.
- The clearest entry profile is **market cap >= $20k, volume >= $2.5k, and buy/sell ratio >= 2.0**: 102 trades, 60.8% win rate, +0.020380 SOL/trade, +2.078793 SOL total. The other 531 entries returned 30.9% wins and +0.002355 SOL/trade.
- Avoid weak early-market entries: any candidate with market cap < $5k, volume < $500, or buy/sell ratio < 0.75 produced 184 trades at a 16.3% win rate and -0.001602 SOL/trade (-0.294853 SOL total).
- UTC 20:00-21:59 is a material dead zone: 73 trades, 20.5% wins, -0.011387 SOL/trade, -0.831245 SOL total. The remaining hours returned +0.007429 SOL/trade.
- The first auto-tune was associated with a large improvement, from -0.015475 SOL/trade before it to +0.007193 after it. Subsequent 50-trade blocks remain positive but fluctuate, so the tuner is directionally useful but not stable evidence that every threshold change helped.
- Crowding matters. At five or more concurrent positions across both strategies, results turned negative: 44 trades, 18.2% wins, -0.006479 SOL/trade.

## 1. Time Of Day (UTC)

| UTC hour | Trades | Win rate | Avg PnL (SOL) | Total PnL (SOL) |
|---:|---:|---:|---:|---:|
| 01 | 31 | 35.5% | +0.004675 | +0.144931 |
| 02 | 40 | 35.0% | +0.007779 | +0.311168 |
| 03 | 22 | 36.4% | +0.003285 | +0.072274 |
| 04 | 16 | 43.8% | +0.008997 | +0.143956 |
| 05 | 28 | 50.0% | +0.012498 | +0.349936 |
| 06 | 26 | 50.0% | +0.009101 | +0.236635 |
| 08 | 10 | 50.0% | +0.014814 | +0.148140 |
| 09 | 11 | 45.5% | +0.008071 | +0.088786 |
| 10 | 13 | 53.8% | +0.011233 | +0.146023 |
| 11 | 9 | 44.4% | +0.010599 | +0.095388 |
| 12 | 17 | 64.7% | +0.025414 | +0.432044 |
| 13 | 23 | 34.8% | +0.009952 | +0.228895 |
| 14 | 25 | 16.0% | -0.005149 | -0.128726 |
| 15 | 50 | 32.0% | +0.006179 | +0.308957 |
| 16 | 41 | 29.3% | +0.005463 | +0.223984 |
| 17 | 46 | 47.8% | +0.010308 | +0.474164 |
| 18 | 36 | 33.3% | +0.006505 | +0.234168 |
| 19 | 4 | 25.0% | -0.001264 | -0.005055 |
| 20 | 24 | 20.8% | -0.020197 | -0.484738 |
| 21 | 49 | 20.4% | -0.007072 | -0.346507 |
| 22 | 66 | 36.4% | +0.006853 | +0.452323 |
| 23 | 46 | 28.3% | +0.004398 | +0.202293 |

**Dead zone:** block new Strategy B entries during 20:00-21:59 UTC. Those hours have a sufficient 73-trade sample and are strongly negative. Hour 14 is also negative (25 trades), but needs more observation before becoming a hard block. Hour 19 has only four trades and is inconclusive.

## 2. Gate Values At Entry

### Winner versus loser averages

| Outcome | Trades | Avg age | Avg mcap | Avg volume | Avg txns | Avg buy/sell |
|---|---:|---:|---:|---:|---:|---:|
| Winner | 226 | 2.25m | $28,417 | $36,763 | 375.6 | 3.48 |
| Loser | 346 | 1.86m | $22,444 | $14,896 | 217.5 | 3.24 |
| Flat | 61 | 4.04m | $8,470 | $8,435 | 196.9 | 2.12 |

### High-signal buckets

| Gate bucket | Trades | Win rate | Avg PnL (SOL) | Interpretation |
|---|---:|---:|---:|---|
| MCap < $5k | 127 | 11.0% | -0.002400 | Consistently poor |
| MCap $20k-$35k | 258 | 39.5% | +0.005966 | Solid core cohort |
| MCap >= $35k | 130 | 54.6% | +0.015222 | Best capitalization cohort |
| Volume < $500 | 29 | 10.3% | -0.001155 | Reject |
| Volume $2.5k-$5k | 91 | 40.7% | +0.007989 | Positive |
| Volume >= $5k | 201 | 47.8% | +0.011713 | Strongest volume cohort |
| Buy/sell < 0.75 | 53 | 24.5% | -0.000964 | Reject |
| Buy/sell 1.5-1.99 | 65 | 38.5% | +0.002230 | Moderate |
| Buy/sell >= 2.0 | 314 | 42.4% | +0.008199 | Strongest momentum cohort |
| Total txns 10-24 | 119 | 23.5% | +0.000010 | Weak, essentially flat |
| Total txns 25-49 | 67 | 44.8% | +0.009209 | Best mid-range bucket |
| Total txns >= 100 | 257 | 39.3% | +0.006619 | Positive, scalable sample |

**Optimal observed profile:** `$20k+` mcap, `$2.5k+` volume, buy/sell `>=2.0`. It delivered 102 trades, 62 wins (60.8%), +0.020380 SOL/trade, and 62.4% of all Strategy B realized PnL. The stricter `$35k+` / `$5k+` / `>=2.0` subset is also strong (58 trades, 56.9%, +0.017439 SOL/trade), but did not improve on the broader profile enough to justify the lower opportunity count.

## 3. Hold Duration

| Hold time | Trades | Win rate | Avg PnL (SOL) | Total PnL (SOL) |
|---|---:|---:|---:|---:|
| Under 30s | 36 | 16.7% | -0.003422 | -0.123210 |
| 30s-1m | 77 | 20.8% | -0.003414 | -0.262897 |
| 1m-2m | 207 | 20.8% | +0.003079 | +0.637277 |
| 2m-5m | 160 | 40.0% | +0.008130 | +1.300808 |
| 5m-15m | 152 | 63.2% | +0.011428 | +1.737060 |
| 15m+ | 1 | 100.0% | +0.040000 | +0.040000 |

**Sweet spot:** 5-15 minutes is the highest-confidence profitable holding window. Sub-minute exits are net-negative and likely identify entries that fail immediately; they are an entry-quality signal rather than evidence that the exits should be delayed. The current early-exit mechanism limits their damage but cannot make the underlying trades profitable.

## 4. Exit Reason Distribution

| Exit reason | Trades | Win rate | Avg PnL (SOL) | Total PnL (SOL) | Avg hold |
|---|---:|---:|---:|---:|---:|
| Hard stop | 264 | 0.0% | -0.017016 | -4.492351 | 2.38m |
| Take profit | 143 | 100.0% | +0.049231 | +7.040000 | 47.13m |
| Early exit, no green | 115 | 5.2% | -0.002283 | -0.262500 | 1.66m |
| Time stop | 101 | 71.3% | +0.010020 | +1.012056 | 10.20m |
| Trailing stop | 10 | 50.0% | +0.003183 | +0.031833 | 1.79m |

Take-profit exits fund the strategy: they are 22.6% of exits and generate +7.04 SOL. Hard stops are 41.7% of exits and cost -4.49 SOL. Early no-green exits are small losses, so their present role as damage containment is supported. The 47-minute average take-profit hold reflects the existing 30-minute time-stop parameter changing later in the sample or old positions; do not infer a recommended holding extension from this descriptive value alone.

## 5. Token Age At Entry

| Entry age | Trades | Win rate | Avg PnL (SOL) | Total PnL (SOL) |
|---|---:|---:|---:|---:|
| Under 5m | 578 | 35.6% | +0.005349 | +3.091903 |
| 5-10m | 29 | 27.6% | -0.000037 | -0.001061 |
| 10-15m | 10 | 70.0% | +0.016366 | +0.163658 |
| 15-20m | 10 | 30.0% | +0.003961 | +0.039613 |
| 20-30m | 6 | 33.3% | +0.005821 | +0.034925 |

Under-5-minute tokens produce nearly all realized PnL because they are 91.3% of observations, but they do **not** outperform older tokens conclusively. The direct requested comparison is under 5m (35.6%, +0.005349) versus 15-30m (16 trades, 31.3%, +0.004659): the younger cohort is modestly better and much better supported by sample size. The 10-15m result is promising but only ten trades. Keep the current age limit; do not tighten it below five minutes on this evidence.

## 6. Auto-Tuner Effectiveness

The initial configuration was age <=30m, mcap >=$2k, volume >=$200, buy/sell >=0.4. The first 54 closed entries before the first automatic update returned 18.5% wins and -0.015475 SOL/trade (-0.835660 SOL). The 579 closed entries after it returned 37.3% wins and +0.007193 SOL/trade (+4.164697 SOL).

| Gate-config block | Trades | Win rate | Avg PnL (SOL) | Total PnL (SOL) |
|---|---:|---:|---:|---:|
| Initial | 54 | 18.5% | -0.015475 | -0.835660 |
| Tune at 50 | 46 | 26.1% | -0.000004 | -0.000204 |
| Tune at 100 | 51 | 39.2% | +0.009190 | +0.468690 |
| Tune at 150 | 51 | 47.1% | +0.007861 | +0.400920 |
| Tune at 200 | 49 | 42.9% | +0.013706 | +0.671576 |
| Tune at 250 | 49 | 28.6% | +0.002050 | +0.100426 |
| Tune at 300 | 52 | 44.2% | +0.009362 | +0.486814 |
| Tune at 350 | 49 | 26.5% | +0.003782 | +0.185295 |
| Tune at 400 | 49 | 42.9% | +0.010319 | +0.505633 |
| Tune at 450 | 51 | 33.3% | +0.006246 | +0.318552 |
| Tune at 500 | 50 | 40.0% | +0.005294 | +0.264684 |
| Tune at 550 | 50 | 34.0% | +0.008630 | +0.431512 |
| Tune at 600 | 32 | 43.8% | +0.010338 | +0.330801 |

**Conclusion:** auto-tuning improved results materially relative to the loose initial configuration, but it oscillates among a few threshold states and lacks a control group. Its winning-entry-only 20th-percentile method does not directly optimize PnL, rejection quality, or changing market regime. Treat the first improvement as evidence for higher minimum quality, not proof that all later automatic changes are causal.

## 7. Position Clustering

Concurrency counts all open positions across Strategies A and B at Strategy B entry time.

| Concurrent positions | Trades | Win rate | Avg PnL (SOL) | Total PnL (SOL) |
|---:|---:|---:|---:|---:|
| 1 | 190 | 41.1% | +0.005431 | +1.031887 |
| 2 | 207 | 35.7% | +0.007564 | +1.565793 |
| 3 | 127 | 36.2% | +0.005845 | +0.742320 |
| 4 | 65 | 30.8% | +0.004185 | +0.272030 |
| 5 | 33 | 18.2% | -0.003550 | -0.117142 |
| 6 | 11 | 18.2% | -0.015077 | -0.165849 |

Results deteriorate as concurrent exposure rises. One to four positions remain positive, while five or more are negative. Strategy B alone shows the same direction (five concurrent B positions: 32 trades, 21.9%, -0.003792 SOL/trade), so this is not just interference from Strategy A.

## Recommendations

1. Block Strategy B entries from 20:00 through 21:59 UTC. This is the strongest time-based signal: 73 observations and -0.831245 SOL total PnL.
2. Raise quality gates toward mcap >=$20k, volume >=$2.5k, and buy/sell >=2.0, initially as a shadow/logged qualification cohort before making it an execution requirement. It is a 102-trade profile with 60.8% wins and +0.020380 SOL/trade.
3. At minimum, reject mcap <$5k, volume <$500, and buy/sell <0.75. The combined weak-quality group is net-negative over 184 trades.
4. Cap total concurrent positions below five. Preserve room for one to four positions, but block new Strategy B entries at a fifth total open position until a dedicated capacity experiment says otherwise.
5. Keep the under-five-minute lane open. It is the only well-powered age cohort and slightly exceeds the 15-30m cohort; apparent 10-15m strength is too small to drive a gate.
6. Preserve the early no-green exit as loss containment, but use a quick early-exit/no-green outcome as feedback to the entry model. The entry filter, not a later exit, is the lever that addresses the 113 mostly losing sub-two-minute closures.
7. Keep auto-tuner changes auditable and evaluate each configuration on the subsequent 50-trade block. Do not assume its oscillating threshold updates are independently beneficial without a control/shadow cohort.

## Method And Limits

- PnL and holding analysis use closed `positions` where `strategy='B'`; all 633 had a matching entered `candidate_log` row.
- Exit reasons are obtained from the linked Strategy B SELL trade's `metadata.close_reason`.
- Concurrent position count is computed from persisted position timestamps. It includes all strategy positions that overlapped the Strategy B entry; timestamp resolution and closure ordering can cause boundary ambiguity.
- Candidate logs show gate-stage rejections but do not contain counterfactual later price paths. This report can identify the realized-entry profile, not prove that every rejected candidate would have lost.
- This is observational data collected over a short, non-contiguous period (2026-08-05 through 2026-08-13). The first-tune comparison is confounded by time and market regime, and small cohorts are labeled accordingly.
- No runtime source, parameter, database, or configuration was changed.
