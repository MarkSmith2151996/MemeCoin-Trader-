=== 30-MINUTE SHADOW TEST ===
Total unique mints seen (Jupiter, ≤22m): 875
Total unique mints seen (DexScreener): 853
Total unique mints seen on BOTH (ever, across all cycles): 853

Cumulative overlap rate: 853 / max(875, 853) = 97.5%

Mints that appeared on Jupiter first: 389
Mints that appeared on DexScreener first: 0
Mints that appeared on both in same cycle: 464
Mints Jupiter-only (never on DexScreener): 22
Mints DexScreener-only (never on Jupiter): 0

=== STRATEGY B GATE PASS COMPARISON ===
Jupiter mints passing all gates (age, mcap≥5K, vol≥500, bsr≥0.5): 77
DexScreener mints passing all gates: 75
Gate-passing mints on BOTH: 53

=== VERDICT ===
Jupiter would have seen 71% of DexScreener's tradeable candidates within the 22m window (53/75).

=== NOTES ===
- DexScreener discovery is lookup-only (browser-pc Cloudflare-blocked, no working trending endpoint): mints NOT on Jupiter are invisible to the DexScreener side, so 'DexScreener-only' counts are zero by construction and both-pass is a lower bound.
- Jupiter volume = stats5m USD; DexScreener volume = h1 USD (unit mismatch as in run_strategy_b.py).
- Gate pass = 'passed at any observed cycle' (best-case), not a single snapshot.
