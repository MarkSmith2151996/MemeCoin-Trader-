# MT-608: Aug. 18-20 Strategy BT Replay vs Live PnL

## Inputs

| Date | Enriched rows | SOL/USD close |
| --- | ---: | ---: |
| 2026-08-18 | 4,104,316 | $77.01 |
| 2026-08-19 | 4,261,779 | $85.34 |
| 2026-08-20 | 4,349,721 | $87.64 |

The enriched partitions were built with the existing archive `etl.build_ohlcv`
and `enrich.enrich_day` functions. The missing SOL/USD closes were obtained
from Coinbase daily candles and added to `D:\pumpapi-replay\derived\sol_prices.csv`.

## PnL Comparison

| Date | BT entries | BT raw PnL (SOL) | BT friction PnL (SOL) | Live closed positions | Live raw PnL (SOL) | Live adjusted PnL (SOL) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 2026-08-18 | 3,526 | +29.359800 | +16.600419 | 205 | +4.296483 | +4.279483 |
| 2026-08-19 | 0 | +0.000000 | +0.000000 | 29 | +0.161105 | +0.062505 |
| 2026-08-20 | 3,487 | +30.188825 | +17.554931 | 535 | +0.761782 | -0.480405 |
| **Total** | **7,013** | **+59.548625** | **+34.155350** | **769** | **+5.219370** | **+3.861583** |

Backtest PnL is allocated by simulated exit time. Live PnL is allocated by
`positions.closed_at` in `data/trades.db` for closed Strategy B positions.
The backtest's friction PnL uses the MT-569 3% entry/exit plus pool-relative
impact model; the live adjusted PnL uses the persisted position adjustment.

Aug. 19 is a Wednesday, so Strategy BT has zero entries under its current
Wednesday gate. The live rows reflect the historical live loop as recorded;
they are not expected to match a gate introduced after those positions ran.

## Outputs

- Enriched partitions: `D:\pumpapi-replay\derived\enriched\2026-08-18.parquet` through `2026-08-20.parquet`
- Replay outputs: `D:\pumpapi-replay\results\capacity_sweep_bt_recent\`
- Replay configuration: `--start 2026-08-18 --end 2026-08-20 --max-open 5`

The archive services were stopped while Aug. 20 was materialized to avoid
concurrent writes, then restarted after the replay inputs were complete.
