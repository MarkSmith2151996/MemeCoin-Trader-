# MT-552 Parameter Sweep — Strategy B Exits

Analyzed **765** closed Strategy B trades (202 snapshot-backed, 563 peak-bound fallback).

## Baseline — current live parameters (4% trail / 80% TP / 10% hard stop, no filter)

| Trades | PnL | WR% | PF | Avg win | Avg loss |
|---:|---:|---:|---:|---:|---:|
| 765 | +6.1831 | 35.9 | 4.17 | +0.0296 | -0.0042 |

## Step 2 — Full exit-parameter sweep (no filter)

Grid: trail {2,3,4,5,6,8}% x TP {60,80,100,120,150}% x hard {8,10,15,20}% (120 combinations).

Top 10 by total PnL (no filter)

| Rank | Trail | TP | Stop | Filter | Trades | PnL | WR% | PF | Avg win | Avg loss |
|---:|---:|---:|---:|---|---:|---:|---:|---:|---:|---:|
| 1 | 2% | 150% | 8% | No filter | 765 | +8.7309 | 35.0 | 6.59 | +0.0384 | -0.0033 |
| 2 | 3% | 150% | 8% | No filter | 765 | +8.7021 | 34.9 | 6.42 | +0.0386 | -0.0034 |
| 3 | 4% | 150% | 8% | No filter | 765 | +8.6757 | 34.6 | 6.24 | +0.0390 | -0.0035 |
| 4 | 5% | 150% | 8% | No filter | 765 | +8.5470 | 34.6 | 6.03 | +0.0387 | -0.0036 |
| 5 | 2% | 150% | 10% | No filter | 765 | +8.4697 | 35.3 | 5.47 | +0.0384 | -0.0041 |
| 6 | 3% | 150% | 10% | No filter | 765 | +8.4408 | 35.2 | 5.35 | +0.0386 | -0.0041 |
| 7 | 6% | 150% | 8% | No filter | 765 | +8.4387 | 34.6 | 5.83 | +0.0384 | -0.0037 |
| 8 | 4% | 150% | 10% | No filter | 765 | +8.4134 | 34.9 | 5.23 | +0.0390 | -0.0042 |
| 9 | 2% | 120% | 8% | No filter | 765 | +8.3763 | 35.0 | 6.36 | +0.0371 | -0.0033 |
| 10 | 3% | 120% | 8% | No filter | 765 | +8.3324 | 34.9 | 6.19 | +0.0372 | -0.0034 |


## Step 3 — Top 5 parameter combos across day/hour filters

Filters: no Wednesday / no UTC 14 / no Wed + no UTC 14 / only Thu+Fri / golden hours (UTC 4-6, 8-12, 17).

Ranked parameter + filter combinations

| Rank | Trail | TP | Stop | Filter | Trades | PnL | WR% | PF | Avg win | Avg loss |
|---:|---:|---:|---:|---|---:|---:|---:|---:|---:|---:|
| 1 | 2% | 150% | 8% | No filter | 765 | +8.7309 | 35.0 | 6.59 | +0.0384 | -0.0033 |
| 2 | 2% | 150% | 8% | Exclude Wednesday | 671 | +8.7545 | 38.9 | 7.63 | +0.0386 | -0.0035 |
| 3 | 2% | 150% | 8% | Exclude UTC 14 | 740 | +8.6472 | 35.8 | 6.80 | +0.0383 | -0.0033 |
| 4 | 2% | 150% | 8% | Exclude Wednesday + UTC 14 | 646 | +8.6708 | 39.9 | 7.95 | +0.0384 | -0.0035 |
| 5 | 2% | 150% | 8% | Only Thursday + Friday | 617 | +8.3046 | 38.7 | 7.62 | +0.0400 | -0.0035 |
| 6 | 2% | 150% | 8% | Only golden hours (UTC 4-6, 8-12, 17) | 240 | +3.7845 | 48.8 | 10.10 | +0.0359 | -0.0036 |
| 7 | 3% | 150% | 8% | No filter | 765 | +8.7021 | 34.9 | 6.42 | +0.0386 | -0.0034 |
| 8 | 3% | 150% | 8% | Exclude Wednesday | 671 | +8.7433 | 38.9 | 7.49 | +0.0387 | -0.0035 |
| 9 | 3% | 150% | 8% | Exclude UTC 14 | 740 | +8.6209 | 35.7 | 6.63 | +0.0385 | -0.0034 |
| 10 | 3% | 150% | 8% | Exclude Wednesday + UTC 14 | 646 | +8.6621 | 39.9 | 7.81 | +0.0385 | -0.0035 |
| 11 | 3% | 150% | 8% | Only Thursday + Friday | 617 | +8.2968 | 38.7 | 7.47 | +0.0401 | -0.0036 |
| 12 | 3% | 150% | 8% | Only golden hours (UTC 4-6, 8-12, 17) | 240 | +3.7448 | 48.8 | 9.86 | +0.0356 | -0.0037 |
| 13 | 4% | 150% | 8% | No filter | 765 | +8.6757 | 34.6 | 6.24 | +0.0390 | -0.0035 |
| 14 | 4% | 150% | 8% | Exclude Wednesday | 671 | +8.7345 | 38.6 | 7.33 | +0.0391 | -0.0036 |
| 15 | 4% | 150% | 8% | Exclude UTC 14 | 740 | +8.5971 | 35.4 | 6.45 | +0.0388 | -0.0035 |
| 16 | 4% | 150% | 8% | Exclude Wednesday + UTC 14 | 646 | +8.6559 | 39.6 | 7.64 | +0.0389 | -0.0036 |
| 17 | 4% | 150% | 8% | Only Thursday + Friday | 617 | +8.2889 | 38.6 | 7.31 | +0.0403 | -0.0036 |
| 18 | 4% | 150% | 8% | Only golden hours (UTC 4-6, 8-12, 17) | 240 | +3.7612 | 48.3 | 9.68 | +0.0362 | -0.0037 |
| 19 | 5% | 150% | 8% | No filter | 765 | +8.5470 | 34.6 | 6.03 | +0.0387 | -0.0036 |
| 20 | 5% | 150% | 8% | Exclude Wednesday | 671 | +8.6244 | 38.6 | 7.12 | +0.0387 | -0.0037 |
| 21 | 5% | 150% | 8% | Exclude UTC 14 | 740 | +8.4713 | 35.4 | 6.23 | +0.0385 | -0.0036 |
| 22 | 5% | 150% | 8% | Exclude Wednesday + UTC 14 | 646 | +8.5487 | 39.6 | 7.43 | +0.0386 | -0.0037 |
| 23 | 5% | 150% | 8% | Only Thursday + Friday | 617 | +8.2077 | 38.6 | 7.12 | +0.0401 | -0.0037 |
| 24 | 5% | 150% | 8% | Only golden hours (UTC 4-6, 8-12, 17) | 240 | +3.7254 | 48.3 | 9.46 | +0.0359 | -0.0038 |
| 25 | 2% | 150% | 10% | No filter | 765 | +8.4697 | 35.3 | 5.47 | +0.0384 | -0.0041 |
| 26 | 2% | 150% | 10% | Exclude Wednesday | 671 | +8.5423 | 39.2 | 6.32 | +0.0386 | -0.0042 |
| 27 | 2% | 150% | 10% | Exclude UTC 14 | 740 | +8.4020 | 36.1 | 5.65 | +0.0382 | -0.0041 |
| 28 | 2% | 150% | 10% | Exclude Wednesday + UTC 14 | 646 | +8.4745 | 40.2 | 6.59 | +0.0384 | -0.0042 |
| 29 | 2% | 150% | 10% | Only Thursday + Friday | 617 | +8.0379 | 38.9 | 6.25 | +0.0399 | -0.0043 |
| 30 | 2% | 150% | 10% | Only golden hours (UTC 4-6, 8-12, 17) | 240 | +3.7065 | 49.2 | 8.36 | +0.0357 | -0.0044 |


## Step 4 — Top 3 step-3 combos on $20K+ mcap trades

Filtered to the 449 trades with mcap >= $20K (the tier carrying the majority of profit).

Top 3 on $20K+ mcap tier

| Rank | Trail | TP | Stop | Filter | Trades | PnL | WR% | PF | Avg win | Avg loss |
|---:|---:|---:|---:|---|---:|---:|---:|---:|---:|---:|
| 1 | 2% | 150% | 8% | Exclude Wednesday | 404 | +7.0351 | 45.5 | 9.83 | +0.0426 | -0.0037 |
| 2 | 3% | 150% | 8% | Exclude Wednesday | 404 | +7.0503 | 45.5 | 9.77 | +0.0427 | -0.0037 |
| 3 | 4% | 150% | 8% | Exclude Wednesday | 404 | +6.9913 | 45.0 | 9.57 | +0.0429 | -0.0038 |


## Step 5 — Overall top 10 (ranked)

Top 10 parameter + filter combinations by total PnL

| Rank | Trail | TP | Stop | Filter | Trades | PnL | WR% | PF | Avg win | Avg loss |
|---:|---:|---:|---:|---|---:|---:|---:|---:|---:|---:|
| 1 | 2% | 150% | 8% | Exclude Wednesday | 671 | +8.7545 | 38.9 | 7.63 | +0.0386 | -0.0035 |
| 2 | 3% | 150% | 8% | Exclude Wednesday | 671 | +8.7433 | 38.9 | 7.49 | +0.0387 | -0.0035 |
| 3 | 4% | 150% | 8% | Exclude Wednesday | 671 | +8.7345 | 38.6 | 7.33 | +0.0391 | -0.0036 |
| 4 | 2% | 150% | 8% | No filter | 765 | +8.7309 | 35.0 | 6.59 | +0.0384 | -0.0033 |
| 5 | 3% | 150% | 8% | No filter | 765 | +8.7021 | 34.9 | 6.42 | +0.0386 | -0.0034 |
| 6 | 4% | 150% | 8% | No filter | 765 | +8.6757 | 34.6 | 6.24 | +0.0390 | -0.0035 |
| 7 | 2% | 150% | 8% | Exclude Wednesday + UTC 14 | 646 | +8.6708 | 39.9 | 7.95 | +0.0384 | -0.0035 |
| 8 | 3% | 150% | 8% | Exclude Wednesday + UTC 14 | 646 | +8.6621 | 39.9 | 7.81 | +0.0385 | -0.0035 |
| 9 | 4% | 150% | 8% | Exclude Wednesday + UTC 14 | 646 | +8.6559 | 39.6 | 7.64 | +0.0389 | -0.0036 |
| 10 | 2% | 150% | 8% | Exclude UTC 14 | 740 | +8.6472 | 35.8 | 6.80 | +0.0383 | -0.0033 |


### Notes
- PnL is simulated SOL PnL from replaying recorded price paths; the current-parameters baseline row is the comparison point.
- Positions without position-linked snapshots use the MT-520 peak-bound approximation (`min(close, peak*(1-trail))` clamped to the hard stop, or TP when the persisted peak reaches it) — exit-parameter results on those trades are approximate.
- Day/hour filters classify by `opened_at` (UTC).
- Read-only analysis; no live parameters, code, or database rows were changed.
