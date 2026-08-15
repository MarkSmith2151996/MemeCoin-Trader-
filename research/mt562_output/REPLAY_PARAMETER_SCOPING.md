# MT-562 Replay Engine Parameter Scoping

Generated: 2026-08-15 18:02 EDT

This is research only. No replay-engine or trading-runtime behavior was changed.

## Evidence And Limits

| Source | What it establishes | Limitation |
| --- | --- | --- |
| `/tmp/strategy_b.log` | Real Jupiter discovery lag and first-sighting-to-entry timing | Current process began at 17:43 EDT, so the sample is young and still growing. Each log line is duplicated because stdout and the file handler both reach the same log. Metrics below dedupe by mint. |
| `data/trades.db` | Current paper-trading configuration, gate history, and paper-data limitations | All Strategy B records are simulated; no real fill exists. |
| `scripts/run_strategy_b.py` | Exact active Strategy B constants and control flow | This is the runtime source of truth, not an old report. |
| `data/parameter_sweep_report.md`, `data/parameter_sweep_results.json` | MT-552 exit-sweep evidence | 563 of 765 original rows used the documented peak-bound fallback rather than full snapshots. |
| `analysis/strategy_b_entry_quality.md` | Historical entry-quality and time-filter cohorts | Observational only; rejected candidates do not have counterfactual outcomes in this dataset. |
| `D:\pumpapi-replay\raw\2026\04\18\00-02.jsonl.zst` | Real PumpApi fee metadata and nearby opposite-side fills | This is a three-hour April 18 sample, not the completed four-month Parquet universe. USD mcap buckets use the repository's July snapshot-implied SOL/USD rate of about $75, so exact bucket labels are approximate. |

The raw archive is fully downloaded (2,877 hourly files, Apr 18 through Aug 15 20:00 UTC), but the ETL is still materializing it. The current ETL state has completed April 18's 24 raw hours and has not finalized the full four-month Parquet dataset. Do not treat a three-hour raw sample as a final fee/impact calibration.

## 1. Discovery Latency

### Measured Jupiter Discovery Lag

`DISCOVERY_LAG` measures `firstPool.createdAt -> first Jupiter API sighting`. The log currently contains 83 distinct mints after deduplicating the duplicated handler output.

| Statistic | Lag |
| --- | ---: |
| Minimum | 4 s |
| Mean | 9.8 s |
| Median | 7 s |
| P25 | 6 s |
| P75 | 9 s |
| P90 | 11 s |
| P95 | 16 s |
| Maximum | 75 s |

### Five-Second Histogram

| Lag bucket | Mints | Share |
| --- | ---: | ---: |
| 0-<5 s | 3 | 3.6% |
| 5-<10 s | 64 | 77.1% |
| 10-<15 s | 11 | 13.3% |
| 15-<20 s | 1 | 1.2% |
| 20-<40 s | 0 | 0.0% |
| 40-<45 s | 1 | 1.2% |
| 45-<50 s | 1 | 1.2% |
| 50-<65 s | 0 | 0.0% |
| 65-<70 s | 1 | 1.2% |
| 70-<75 s | 0 | 0.0% |
| 75-<80 s | 1 | 1.2% |

The central distribution is fast: 90.4% of observed mints appeared within 15 seconds. Four mints (4.8%) formed a long 42-75 second tail. This is enough to meet the task's 50-sample threshold, but not enough to call the tail stable. MT-563's `discovery_lag` table/report will become the durable source after Strategy B is restarted; the currently running process predates that commit and therefore logs lag but has not created the table.

### Important: Discovery Is Not The Whole Entry Delay

There are nine distinct `LATENCY` records in the same log. They show:

| Component | Min | Median | Mean | Max |
| --- | ---: | ---: | ---: | ---: |
| First sighting -> full gate pass | 45.7 s | 46.5 s | 46.4 s | 47.0 s |
| Full entry pipeline | 46.0 s | 46.8 s | 46.7 s | 47.2 s |

The 45-second `SCREEN_COOLDOWN_S` explains this: a fresh mint is commonly first screened before volume/transaction gates pass, then re-screened about 45 seconds later. Quote, send, and paper-confirm steps were only roughly 0.3 seconds combined. The replay should model this gate maturation/cooldown behavior, not treat it as a generic network delay.

### Recommended Replay Latency Scenarios

Use independent controls instead of one arbitrary static delay:

| Scenario | Discovery buffer | Screening behavior | Why |
| --- | ---: | --- | --- |
| Typical | 7 s | Evaluate gates immediately, then respect the 45 s re-screen cooldown after a failure | Observed median discovery lag; reproduces runtime semantics. |
| Conservative | 10 s | Same 45 s re-screen cooldown | Observed P75. |
| P90 | 16 s | Same 45 s re-screen cooldown | Observed P95 is 16 s; captures nearly all central observations. |
| Tail stress | 45 s and 60 s | Same cooldown | Covers the currently observed 42-75 s long tail without pretending it is the normal case. |

For a fixed-delay sensitivity check only, report full first-seen-to-entry delays of about 54 s, 57 s, and 63 s (`7/10/16 s discovery + 46.5 s observed gate pass`). The engine's primary implementation should instead replay the actual rule: first appearance, gate evaluation, then retry every 45 seconds while the mint remains eligible.

## 2. Slippage And Fees

### What The Paper Database Can And Cannot Measure

It cannot measure realized buy or sell slippage:

- All Strategy B trades are `mode='paper'` and `status='simulated'`.
- The paper trade's `price_sol` is its simulated mark, not an on-chain fill versus an intended price.
- `slippage_bps=300` is a configured simulated/tolerance field, not observed execution friction.
- `jupiter_quotes` contains zero rows. MT-538 records that `quote-api.jup.ag` was unreachable, so the shadow Jupiter quote path has not produced a price-versus-paper sample.

The replay must therefore use archived pool/trade evidence, not infer live execution quality from the paper ledger.

### PumpApi Fee Evidence

Across the April 18 00:00-02:59 UTC raw sample, the median archived `poolFeeRate` by pool was:

| Pool | Median fee per swap | Approximate two-leg fee component |
| --- | ---: | ---: |
| `pump` bonding curve | 1.25% | 2.50% |
| `pump-amm` | 0.30% | 0.60% |
| `meteora-damm-v2` | 0.01% | 0.02% |
| `raydium-cpmm` | 0.30% | 0.60% |
| `raydium-launchpad` | 1.50% | 3.00% |

The approximately 1% pump.fun protocol/creator fee mentioned in the task is directionally correct, but the archive's active `pump` rows report 1.25% per swap. The replay should read and apply each event/pool's recorded `poolFeeRate`; it should not hard-code a universal 1% fee.

The same $5K-$50K April sample was not mostly bonding-curve-only: 50.9% of trades were `meteora-damm-v2`, 32.7% `pump-amm`, 14.3% `pump`, and the remainder launchpad/other pools. A mcap-only model is therefore a fallback. Pool-aware fees are required for a credible full-universe replay.

### Nearby Opposite-Side Fill Measurement

Method: for each mint/pool in the three-hour sample, compare consecutive opposite-side fills within two seconds and keep Strategy B-sized buy amounts (0.05-0.10 SOL). The difference between marginal prices is an impact/spread proxy; it also contains unavoidable sub-two-second market movement, so use medians for base calibration and p90 only as stress evidence.

| MCap tier (USD at $75/SOL) | Pairs | Median impact/spread | P90 |
| --- | ---: | ---: | ---: |
| <$10K | 8,330 | 0.50% | 8.73% |
| $10K-$50K | 17,619 | 0.21% | 1.84% |
| $50K-$100K | 20,165 | 0.13% | 1.36% |
| $100K+ | 30,031 | 0.34% | 0.96% |

The <$10K p90 is intentionally not a base assumption: it includes rapid real price movement between observed trades, not just deterministic order impact. It is a valid stress case.

### Recommended Slippage Model

Implement pool-aware slippage first:

1. Fill at the replayed pool price at the decision timestamp.
2. Apply the archived `poolFeeRate` on each synthetic leg using the replay's side-aware pool formula.
3. Apply an impact buffer based on mcap only when the exact pool state cannot supply it.
4. Keep base, conservative, and severe-stress scenarios fixed across parameter sweeps. Do not tune a strategy to the most favorable friction assumption.

Recommended fallback, expressed as total round-trip cost when pool-specific simulation is unavailable:

| MCap tier | Base round trip | Stress round trip | Evidence and use |
| --- | ---: | ---: | --- |
| <$10K | 3.0% | 5.0% | `pump` fee component is 2.5%; observed median impact is about 0.5%. This validates the existing 3% baseline. |
| $10K-$50K | 2.0% | 3.0% | Mixed AMM/curve pool population; median nearby impact is 0.21%. Pool fee is the major differentiator. |
| $50K-$100K | 1.5% | 2.5% | More migrated AMM liquidity; median impact is 0.13%. |
| $100K+ | 1.0% | 2.0% | Deeper pools; median impact remains below 0.5%. |

Pool-specific defaults should override that fallback:

| Pool class | Base round trip | Stress | Reason |
| --- | ---: | ---: | --- |
| `pump` bonding curve | 3.0% | 5.0% | 1.25% recorded fee on each leg plus small-cap impact. |
| `pump-amm` / `raydium-cpmm` | 1.5% | 3.0% | Typical 0.30% per-leg pool fee plus observed short-window movement. |
| `meteora-damm-v2` | 1.0% | 2.5% | Very low recorded fee, but launch-pool movement still matters. |
| Unknown / malformed pool | 3.0% | 5.0% | Conservative fallback until the route is classified. |

`data/analytics_report.md` currently applies 8% below $10K, 5% for $10K-$20K, and 3% at $20K+. Keep that table as a severe break-even stress sensitivity. It is substantially more conservative than the limited raw-event median evidence and should not be the replay's sole/base model.

## 3. Exact Current Strategy B Baseline

### Discovery And Entry Gates

| Parameter / gate | Active value | Runtime behavior |
| --- | --- | --- |
| Discovery source | Jupiter Tokens V2 `/toporganicscore/5m?limit=100` plus `/recent?limit=30` | Deduplicates by mint; endpoints are 250 ms apart. |
| Source age / max age | <=22 min | Requires `firstPool.createdAt`; source and full screen both use this limit. |
| MCap band | $5,000 <= mcap <= $50,000 | Lower bound comes from `GATES`; upper bound is `MAX_MCAP_USD`. |
| Transaction count | Age-adjusted H1 buys+sells | `<1m: 3`, `1-<3m: 5`, `3-<5m: 8`, `5-<10m: 12`, `>=10m: 16`; absolute `MIN_TXNS=3`. |
| H1 volume | >=$500 | Uses Jupiter `stats1h.buyVolume + sellVolume`. |
| Volume/MCap | 0.005 <= H1 volume/mcap <= 50.0 | Rejects low ratio as `dead_volume`; high ratio as `wash_trading`. |
| Low-fee proxy | estimated fees `txns * 0.001` versus `(mcap/15000)*0.3 SOL` | Warning only in paper mode; not part of `all_pass`. |
| Buy/sell ratio | >=0.5 | `buys / max(sells, 1)`. |
| RugCheck availability | Report must be found; timeout/provider_error/http_429 rejects | No bypass on provider error. |
| Mint/freeze authority | Both must not be explicitly `False` | An explicit live mint or freeze authority fails; unknown values currently pass this check. |
| Top-holder hard cap | <=100% | `HOLDER_TIERS` warn at 30% at every age but hard-reject remains 100%, effectively disabling concentration rejection. |
| Creator holdings | <=10% when present | Missing creator holdings logs a warning and passes. |
| Mentions | Disabled: `REQUIRE_MENTIONS=False` | If enabled later: raw mentions >=3 in first 5 min; influencer mode is also disabled. |
| Blocked UTC hours | `{0, 7, 19, 20, 21}` | Blocks after full screen, before entry. |
| Blocked weekday | Wednesday, Python weekday `2` | Blocks after the hour gate. |
| Repeat loser | Permanent skip for any mint with a prior negative close | Checked before quote/entry. |
| Existing position | One open position per mint | Dedupe check. |
| Position capacity | `MAX_OPEN=5` | Entry blocks when five Strategy B paper positions are open. |
| Base size | 0.05 SOL | Saturday multiplier is 0.5, so Saturday size is 0.025 SOL. |
| Scan cadence | 2 s | `STRATEGY_B_SCAN_INTERVAL`, default 2. |
| Re-screen cooldown | 45 s | Failed/seen candidates cannot be fully re-screened more often. |
| Seen-mint TTL | 1 hour | Mint is not re-evaluated for entry after a full pass/attempt during this TTL. |

`MAX_MCAP_RUGCHECK=50_000` is declared but not read by the current runtime. It is not an additional active gate. Browser/DexScreener constants and `MAX_SOURCE_ROWS=30` belong to the deprecated discovery fallback, not the active Jupiter source.

### Exit And Monitoring Parameters

Exit precedence is take profit -> hard stop -> trailing stop -> early no-green -> time stop.

| Parameter | Active value | Runtime behavior |
| --- | ---: | --- |
| Monitor interval | 30 s | Open paper positions are marked on this cadence. |
| Fast monitor interval | 5 s | Next monitor speeds up when mark is below entry by 5%. |
| Take profit | +150% | Closes at the TP threshold price. |
| Hard stop | -8% | Closes at the hard-stop threshold price. |
| Trailing arm | +2% | Trail is inactive until peak exceeds this threshold. |
| Trailing stop | 2% from peak | Closes at current mark after arm. |
| Early no-green confirmation | 90 s | Applies only if peak is <= +1%. |
| Early no-green threshold | +1% | A position that never exceeds this by 90 s closes. |
| Time stop | 10 min | Closes at current mark when no earlier exit fired. |

### Current `gate_config` Rows

The DB's latest row (ID 16, `2026-08-15T00:32:17Z`, reason `parameter_sweep_MT552`) is the exit/day filter audit record:

```json
{
  "trail_stop_pct": 0.02,
  "trailing_arm_pct": 0.02,
  "take_profit_pct": 1.5,
  "hard_stop_pct": 0.08,
  "blocked_weekdays": [2]
}
```

The current entry-gate audit row is ID 15 (`manual_freeze`):

```json
{
  "max_age_minutes": 22,
  "min_mcap_usd": 5000,
  "min_volume_usd": 500,
  "min_buy_sell_ratio": 0.5
}
```

Earlier `gate_config` history moved from the initial `30m/$2K/$200/0.4` through auto-tuned states spanning 9.49-22.5 minutes, $1,250-$4,883 mcap, $250-$625 volume, and 0.5-0.98 buy/sell. MT-552 then set the current 2%/150%/8% exits and Wednesday block.

## 4. Recommended Sweep Ranges

Run slippage/latency scenarios as fixed outer conditions. Do not select a signal parameter because it wins only at unrealistically low friction.

| Parameter | Current | Sweep range | Step / levels | Rationale |
| --- | ---: | --- | --- | --- |
| Discovery buffer | 7 s median observed | 7, 10, 16 s; 45/60 s tail stress | fixed scenarios | Direct discovery-lag percentiles; tail remains small but real. |
| Re-screen cooldown | 45 s | 15-60 s | 15 s | It creates the observed ~46 s gate-pass delay and changes signal maturation. |
| Max token age | 22 min | 10-30 min | 2 min | History tested 9.49-30; entry-quality data does not justify tightening below 5 min. |
| Min mcap | $5K | $5K-$35K | $5K | <$5K was negative; $20K+ was strongest, and $35K+ was also positive but smaller. |
| Max mcap | $50K | $25K-$100K | $25K | Tests the early-entry cap without making the universe mostly mature/migrated tokens. |
| Min H1 volume | $500 | $500-$10K | $500 | MT-531 found $2.5K+ positive and $5K+ strongest; do not revisit below $500. |
| Buy/sell ratio | 0.5 | 0.5-2.5 | 0.25 | Historical quality rises materially at >=2.0; history also tested 0.4-0.98. |
| Age-adjusted txns | 3/5/8/12/16 | 0.75x, 1.0x, 1.5x, 2.0x of the whole schedule | schedule multiplier | Keep the age structure; independently sweeping five thresholds would overfit. |
| Min volume/mcap | 0.005 | 0.001-0.020 | 0.0025 | Tests dead-volume protection while retaining current threshold. |
| Max volume/mcap | 50.0 | 10, 25, 50, 100 | discrete | Tests the wash-trading cap without assuming its current loose cap is useful. |
| Creator holdings cap | 10% when known | 5%-20% | 5% | Current missing-data behavior must remain a separate category. |
| Top-holder hard cap | 100% | 60%, 80%, 100% | discrete | Current value is effectively disabled; sweep cautiously because historical outcomes lack counterfactual rejected paths. |
| Low-fee proxy | warning only | ignore, warn, hard-block | categorical | It is non-blocking now; require enough historical raw evidence before promoting it to a hard gate. |
| Wednesday | blocked | block, allow | categorical | MT-552 found 94 Wednesday trades at 21.3% WR/-0.88 SOL, but validate once out of sample. |
| Hour filter | `{0,7,19,20,21}` blocked | baseline; add UTC 14; remove UTC 19; no extra filter | categorical sets | MT-552's no-UTC-14 cohort was positive; avoid a 24-hour combinatorial search. |
| Max open positions | 5 | 1-4 | 1 | MT-531 found five or more concurrent positions negative. |
| Trail | 2% | 1%-6% | 1% | MT-552 clustered at 2%-5% and current winner is 2%. |
| Trailing arm | 2% | 0%-5% | 1% | Must be evaluated with trail, not alone. |
| Take profit | 150% | 75%-225% | 25% | MT-552's winner sat at its tested 150% upper edge; extend upward modestly before claiming 150 is optimal. |
| Hard stop | 8% | 5%-15% | 1% | MT-552 tested 8/10/15/20 and ranked 8% best. |
| Early confirmation | 90 s | 60-180 s | 30 s | Preserve the current damage-control mechanism while testing reasonable responsiveness. |
| Early green threshold | 1% | 0%-3% | 0.5% | Test jointly with confirmation; both define the same exit behavior. |
| Time stop | 10 min | 5-20 min | 5 min | Current value is fixed in MT-552; historical hold-duration evidence supports testing nearby values only. |
| Slippage scenario | pool-aware, fallback 3/2/1.5/1% | base, conservative, severe | fixed scenarios | Economic assumption, not an optimization target. |

### Joint Versus Independent Sweeps

| Sweep group | Evaluate jointly | Keep out of the same Cartesian product |
| --- | --- | --- |
| Exit mechanics | trail, arm, take profit, hard stop | Entry gates and hour filters. MT-552 already demonstrated strong interaction among exit levels. |
| Early exits | confirmation, green threshold, time stop | Primary exit sweep until the top exit candidates are selected. |
| Entry quality | min mcap, max mcap, volume, buy/sell, txns schedule | Holder/creator filters until the replay has trustworthy historical risk-state inputs. |
| Time/day | Wednesday and a few predeclared hour-filter sets | All 24 hours independently; that is a multiple-testing trap. |
| Runtime realism | discovery buffer, re-screen cooldown, poll/mark cadence, pool-aware friction | Signal-quality parameters; these should be sensitivity conditions, not PnL-tuned knobs. |

Recommended sequence: freeze the current entry settings and sweep exits under all base/conservative friction scenarios; then sweep entry-quality parameters with the selected exit family; then test the limited day/hour sets on only the few surviving configurations. Lock all choices before the final holdout.

## 5. Train And Holdout Recommendation

Use a strict chronological split. The raw archive is Apr 18 00:00 UTC through Aug 15 20:00 UTC. Do not use the final partial Aug 15 day as a normal daily validation sample.

| Partition | UTC date range | Use |
| --- | --- | --- |
| Development / sweep universe | Apr 18-Jul 14 inclusive (88 full days) | All parameter design, latency/friction scenarios, and inner walk-forward validation. |
| Locked final holdout | Jul 15-Aug 14 inclusive (31 full days) | One final, untouched chronological evaluation after parameters are locked. |
| Post-holdout partial data | Aug 15 00:00-20:00 | Optional operational smoke check only; do not mix with the full-day holdout statistics. |

Inside the 88-day development range, use expanding chronological folds rather than random rows:

| Fold | Train through | Validate on |
| --- | --- | --- |
| 1 | Apr 18-May 31 | Jun 1-Jun 15 |
| 2 | Apr 18-Jun 15 | Jun 16-Jun 30 |
| 3 | Apr 18-Jun 30 | Jul 1-Jul 14 |

For each fold, assign a token by its first eligible entry timestamp and enforce a 22-minute boundary embargo. This prevents a mint discovered before the split from carrying its pre-split age/flow information into the next partition. Any position that would cross a partition boundary should be closed/censored at that boundary under the replay's defined mark rule; do not read its later path while training.

Run a day-stratified out-of-sample diagnostic only inside the development set (for example, hold out complete weekdays/hours across the whole period). Compare it to the chronological result, but select the model only if it survives chronological date OOS. This directly addresses the stated Reddit warning: a static/trailing target that wins only when days are mixed can be exploiting a regime distribution that is unavailable in the future.

The archive volume is ample for date splitting: April 18 00:00 alone contains 927,340 buy/sell events, 5,613 active mints, and 1,730 unique mints that traded in the approximate $5K-$50K band. Gate-qualified entries will be much fewer, so require a minimum number of entries per configuration/fold before ranking it and report confidence intervals or bootstrap intervals for net PnL and win rate.

## Bottom Line

1. Model discovery at 7/10/16 seconds with 45/60-second tail stress, but replay the 45-second re-screen cooldown explicitly because it currently dominates first-sighting-to-entry time.
2. Paper trades provide no realized fill measurement. Use PumpApi `poolFeeRate` and pool state, not `trades.slippage_bps=300`, for replay friction.
3. The raw sample supports the longstanding 3% small-cap pump.fun round-trip baseline: 1.25% archived fee per curve swap plus about 0.5% nearby-trade impact. Use pool-aware fees before falling back to mcap tiers.
4. The exact current baseline is $5K-$50K, <=22m, H1 volume >=$500, age-adjusted 3/5/8/12/16 txns, buy/sell >=0.5, 2% trail/arm, 150% TP, 8% hard stop, 90s/+1% early exit, 10m time stop, five-position cap, blocked UTC 0/7/19/20/21, and Wednesday block.
5. Develop on Apr 18-Jul 14, lock parameters, and use Jul 15-Aug 14 as one untouched chronological holdout. Date OOS is the decision gate; day-stratified OOS is a diagnostic only.
