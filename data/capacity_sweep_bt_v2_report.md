# MT-678: V2 Strategy BT Full-History Backtest

Replay range: **2026-04-18 through 2026-08-21 UTC** (126 complete archive days).
Hive configuration captured read-only at: `2026-08-28T18:24:00.837856+00:00`.

## Auditable live V2 parameters

| source | parameter | value |
| --- | --- | ---: |
| `memecoin.gate_config` | age_offset_seconds | `39` |
| `memecoin.gate_config` | blocked_hours_utc | `[0,19,20,21]` |
| `memecoin.gate_config` | blocked_weekdays | `[2]` |
| `memecoin.gate_config` | creator_holdings_max | `0` |
| `memecoin.gate_config` | max_age_seconds | `1320` |
| `memecoin.gate_config` | max_open | `5` |
| `memecoin.gate_config` | max_top_holder_pct | `100` |
| `memecoin.gate_config` | max_volume_to_mcap_ratio | `50` |
| `memecoin.gate_config` | mcap_ceiling | `50000` |
| `memecoin.gate_config` | mcap_floor | `5100` |
| `memecoin.gate_config` | min_age_seconds | `22` |
| `memecoin.gate_config` | min_buy_sell_ratio | `0.5` |
| `memecoin.gate_config` | min_pool_sol_bonding | `5` |
| `memecoin.gate_config` | min_pool_sol_graduated | `5` |
| `memecoin.gate_config` | min_volume_to_mcap_ratio | `0.005` |
| `memecoin.gate_config` | min_volume_usd | `500` |
| `memecoin.gate_config` | score_threshold_bonding | `40` |
| `memecoin.gate_config` | score_threshold_graduated | `40` |
| `memecoin.gate_config` | txn_count_adjustment | `1.24` |
| `memecoin.exit_config` | hard_stop_pct | 8 |
| `memecoin.exit_config` | take_profit_pct | 150 |
| `memecoin.exit_config` | time_stop_minutes | 10 |
| `memecoin.exit_config` | trailing_arm_pct | 2 |
| `memecoin.exit_config` | trailing_stop_pct | 2 |
| live environment | POSITION_SIZE_SOL | 0.02 |
| live configuration | MAX_OPEN | 5 |

The age-tier transaction requirement remains executable logic in `services.data_collector._age_adjusted_min_txns`: 3 / 5 / 8 / 12 / 16 at corrected ages <1 / <3 / <5 / <10 / >=10 minutes. The replay applies the Hive `txn_count_adjustment` before that comparison.

## Execution model

| component | applied model | source |
| --- | --- | --- |
| entry delay | 42.5s; first completed archive bar at or after the delay fills at its close | MT-594 median; 5-second archive granularity |
| entry slippage | 0.187% plus position_size / entry_pool constant-product price impact | MT-594; `src/strategy/position_manager.py` |
| exit fill | next completed archive bar close after hard-stop, take-profit, trailing, or time trigger; 0.187% plus position_size / exit_pool impact | MT-678 execution rule; `services/executor.py` exit priority |
| DEX fee | 1.00% of position size per leg | `src/strategy/position_manager.py:DEX_FEE_PCT` |
| priority fee | 0.0002 SOL per leg (0.0004 SOL round trip) | `src/strategy/position_manager.py:PRIORITY_FEE_PER_LEG` |

Raw PnL uses the delayed entry and delayed next-bar exit prices before costs. Net PnL uses the resulting token quantity after slippage/AMM impact, subtracts both 1% DEX legs and both priority fees. Both scenarios cap full-liquidation proceeds at the contemporaneous exit SOL reserve, so malformed/stale archive marks cannot imply an impossible withdrawal. Net PnL is not comparable to MT-606's flat 3% model.

### Interpretation warning

The mean PnL can be dominated by a small number of extreme archived take-profit and trailing paths even after the pool-reserve bound. The median net day is the more stable summary in this archive and may be negative while the mean is positive. These results describe the archived bar/fill surface, not a validated live-profit forecast; no arbitrary return cap was added because the archive cannot ground one in measured fill evidence.

Exit priority was traced through `StrategyExecutor._monitor_position_locked` and `StrategyExecutor._exit_reason`: peak/arm state updates first, then hard stop, take profit, trailing stop, and time stop. V2 paper code would otherwise fill fixed levels through `_paper_exit_price`; this replay intentionally supersedes that fill behavior with the required next-bar execution model.

## Full-range results

| metric | perfect visibility | realistic visibility |
| --- | ---: | ---: |
| entries | 146,356 | 146,088 |
| raw win rate | 60.41% | 60.40% |
| net win rate | 35.16% | 35.07% |
| raw PnL (SOL) | +39320.115731 | +39127.653499 |
| net PnL (SOL) | +39016.244666 | +38824.175777 |
| raw daily mean (SOL) | +312.064411 | +310.536933 |
| net daily mean (SOL) | +309.652735 | +308.128379 |
| raw daily median (SOL) | +0.001248 | +0.007453 |
| net daily median (SOL) | -1.034958 | -1.001172 |
| raw worst day | -1.446511 | -1.455709 |
| net worst day | -3.318871 | -3.183383 |
| capacity-blocked scans | 1,536,070 | 1,535,167 |
| raw worst-day date | 2026-07-14 | 2026-07-14 |
| net worst-day date | 2026-07-24 | 2026-07-24 |

## Exit-reason breakdown: perfect visibility

| exit reason | count | raw WR | net WR | net WR contribution | raw PnL | net PnL | raw avg/trade | net avg/trade |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| take_profit | 1,271 | 54.21% | 52.56% | 0.46pp | +36963.423415 | +36805.914081 | +29.082158 | +28.958233 |
| trailing_stop | 105,112 | 79.05% | 45.30% | 32.53pp | +2366.038112 | +2269.943689 | +0.022510 | +0.021595 |
| hard_stop | 35,097 | 5.27% | 2.81% | 0.67pp | -10.190441 | -45.194360 | -0.000290 | -0.001288 |
| time_stop | 4,876 | 57.05% | 44.93% | 1.50pp | +0.844644 | -14.418744 | +0.000173 | -0.002957 |

## Exit-reason breakdown: realistic visibility

| exit reason | count | raw WR | net WR | net WR contribution | raw PnL | net PnL | raw avg/trade | net avg/trade |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| take_profit | 1,256 | 54.78% | 52.71% | 0.45pp | +36878.854229 | +36721.406265 | +29.362145 | +29.236788 |
| trailing_stop | 104,836 | 79.09% | 45.24% | 32.47pp | +2239.791501 | +2143.863228 | +0.021365 | +0.020450 |
| hard_stop | 35,117 | 5.30% | 2.81% | 0.68pp | +8.548548 | -26.468302 | +0.000243 | -0.000754 |
| time_stop | 4,879 | 56.65% | 44.27% | 1.48pp | +0.459222 | -14.625414 | +0.000094 | -0.002998 |

## Realistic-visibility model

| metric | result |
| --- | ---: |
| poll size | 30 tokens / 5-second bar |
| simulated polls | 2,176,005 |
| born-token discovery coverage | 97.72% |
| median of daily discovery medians | 11.88s |

The realistic pass uses MT-613's weighted 30-token poll, 120-second newborn floor, and one-way discovery/watch-list model. Perfect visibility evaluates every replayable gate-passing archive observation.

## Comparison with MT-606 baseline

| scenario | entries delta vs 282,924 | raw WR delta vs 68.92% | net PnL delta vs +1,147.32 SOL |
| --- | ---: | ---: | ---: |
| perfect_visibility | -136,568 | -8.51pp | +37868.924666 |
| realistic_visibility | -136,836 | -8.52pp | +37676.855777 |

MT-606 used the older gate snapshot, 0.05 SOL standard / 0.025 SOL Saturday sizing, a flat 3% entry/exit friction model, fixed-level exits, and ended on 2026-08-17. This run uses V2's Hive gates, 0.02 SOL no-Saturday-multiplier sizing, 42.5-second delayed entry, next-bar exits, measured per-leg costs, score ordering, hard-stop-only 24-hour repeat-loser ban, and four additional archive days. Those gate, scheduling, execution-cost, and data-range changes jointly explain movement; this is not an apples-to-apples parameter-only delta.

## Replay limits

- Historical RugCheck/Jupiter-audit reports are absent. The archive's older authority and   top-10-holder fields are not equivalent to V2's timestamped `mint_authority_revoked`,   `freeze_authority_revoked`, and single-holder `top_holder_pct` evidence, so those live   gates are explicitly omitted rather than assumed to pass.
- The archive supplies aggregate buy/sell SOL volume and total trades, not Jupiter's 1-hour   buy/sell transaction counts. Dollar volumes are reconstructed with the archive's daily   SOL/USD series and total transactions use the live 1.24 adjustment.
- Archive close marks are a 5-second proxy for the V2 PumpPortal/Jupiter monitor. It cannot   reproduce sub-bar marks, mark outages/SLA exits, actual route quotes, failed sells, or   live quarantines.
- Pool labels approximate V2 first-pool classification: `pool == pump` while not graduated   is treated as bonding; all other rows are treated as graduated.
- The V2 24-hour repeat-loser behavior is modeled as the persisted hard-stop ban in   `services.strategy`/`services.store`; non-hard-stop losing exits do not create a ban.
