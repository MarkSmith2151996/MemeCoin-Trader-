# MT-607 Backtest Alignment Diff

`scripts/capacity_sweep_bt.py` is the source of truth for replayable Strategy B
entry gates and exits. This document records the differences found in
`scripts/run_strategy_b.py` and the resulting alignment.

## Screening Gates

| Gate or parameter | Backtest | Live before MT-607 | Live after MT-607 |
| --- | --- | --- | --- |
| Age window | 0-22 minutes | <=22 minutes; a future timestamp could pass | 0-22 minutes |
| Market cap | $5,100-$50,000 | $5,100-$50,000 | $5,100-$50,000 |
| Pool floor | 5 SOL, bonding and graduated | 5 SOL, bonding and graduated | 5 SOL, bonding and graduated |
| Transaction floor | 3/5/8/12/16 at <1/<3/<5/<10/>=10 min | Same | Same |
| Volume floor | $500 | $500 | $500 |
| Volume/mcap | 0.005-50.0 | 0.005-50.0 | 0.005-50.0 |
| Buy/sell ratio | cumulative buy volume / sell volume >=0.5 | buy transaction count / sell transaction count >=0.5 | Jupiter 1h buy volume / sell volume >=0.5 |
| Score formula | 40% buy/sell, 30% vol/mcap, 15% txns, 15% volume; threshold 40 | Same weights/threshold, but buy/sell component used transaction counts | Same weights/threshold with buy/sell volume input |
| Low-fee check | warning only | warning only | warning only |
| Creator holdings | reject >0% when known | Same | Same |
| UTC blocks | hours 0/19/20/21 and Wednesday | top-loop and entry checks only | top-loop, `screen_coin`, and entry checks |
| Mint/freeze authority | unavailable in archive; omitted | revoked authorities required | unchanged live-only provider gate |
| Top-holder concentration | unavailable in archive; omitted | <=100% when reported | unchanged live-only provider gate |

The backtest's historical archive does not contain RugCheck reports. The two
live-only RugCheck checks remain required in the live loop and are not treated
as replay-equivalent data.

## Exit Rules

| Exit | Backtest | Live before MT-607 | Live after MT-607 |
| --- | --- | --- | --- |
| Take profit | >=2.5x; close at 2.5x | >=2.5x; close at 2.5x | identical |
| Hard stop | <=0.92x; close at 0.92x | <=0.92x; close at 0.92x | identical |
| Trailing arm | peak >=1.02x | peak >1.02x | peak >=1.02x |
| Trailing stop | current <=98% of peak; close at 98% of peak | >=2% drawdown; close at observed mark | current <=98% of peak; close at 98% of peak |
| Time stop | 10 minutes | 10 minutes | identical |
| Early exit | none | close after 90s with no green move | removed |

The live loop retains its existing no-price time-stop close. This is an
operational safety path for unavailable live marks; the backtest always has a
historical bar and cannot represent that condition.

## Position Sizing

- Standard size: 0.05 SOL in both paths.
- Saturday size: 0.025 SOL in both paths.
- Maximum open positions: 5 in both paths.

All replayable screening, position-sizing, and priced-exit parameters now
match `capacity_sweep_bt.py`.
