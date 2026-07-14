# MT-444: Momentum Tuned V3 Paper Batch (15s Marks)

## Configuration
- **Mark interval**: 15 seconds (MT-444 change from 60s)
- **Exits**: MT-439 tuned (hard stop -20%; activation +10%; standard trail 8%; tightened trail +25%/5%)
- **Max entries**: 3
- **Size**: 0.01 SOL
- **Max hold**: 30 minutes
- **Source**: DexScreener New Pairs UI capture (same as MT-439)

## Results

| Trade | Mint | Entry | Peak | Exit | Reason | Hold | PnL (SOL) | Return |
|-------|------|-------|------|------|--------|------|-----------|--------|
| 1 | GUkMvcxXPv5B... | 0.0000001887 | 0.0000001893 | 1.456e-07 | hard_stop_loss | 1547s | -0.002284 | -22.84% |
| 2 | 4p37V4YA54Jo... | 0.0000009990 | 0.0000014700 | 1.373e-06 | trailing_stop_tight | 895s | +0.003744 | +37.44% |
| 3 | 9w47d5K1Whg4... | 0.0000205000 | 0.0000205000 | 1.536e-05 | hard_stop_loss | 314s | -0.002507 | -25.07% |

## Summary
- **Total trades**: 3 (1 win, 2 losses)
- **Net realized PnL**: -0.001048 SOL
- **Net vs MT-439** (-0.001133 SOL): marginally better by +0.000085 SOL

## 15s Marks Observations
1. **Trade 1**: 102 marks over 25.8 min. Captured slow decline with high granularity. Hit hard stop at -22.84%. Gap between -19.2% and -22.8% was missed at 60s cadence.
2. **Trade 2**: Precise peak capture at $1.47e-6 (+47.1%). Tight trail locked at +37.44% on decline. Plateau at peak was visible across multiple 15s marks before decline began.
3. **Trade 3**: Fell from -14% to -25% in 60s (a -11% gap). 15s marks caught the final hard stop exactly.

## Verdict
15s marks provide better peak-capture resolution and more precise stop-loss fills but did not materially improve PnL on this batch. The dominant loss source remains gap moves between marks rather than mark frequency.
