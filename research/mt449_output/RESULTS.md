# MT-449: High-Liquidity 5-Second Paper Comparison

## Candidate Search Results

| Source | Solana mints |
|--------|-------------|
| DexScreener token-profiles latest/v1 | 15 |
| DexScreener token-boosts top/v1 | 18 |
| DexScreener token-boosts latest/v1 | 32 |
| **Total unique** | **46** |
| Valid SOL/wSOL pairs | 44 |
| **Meeting ALL filters** | **0** |

## Filters Applied

| Filter | Threshold | Candidates passing |
|--------|-----------|-------------------|
| Chain = Solana | solana | 46/46 |
| Pair = mint/wSOL | baseToken=requested, quoteToken=wSOL | 44/46 |
| Age <= 1 hour | <= 3600s | 8/44 |
| Liquidity > $50K | > 50000 USD | 11/44 |
| Volume > $1K | > 1000 USD 24h | 44/44 |
| Market cap > $7K | > 7000 USD FDV | 29/44 |
| **All combined** | | **0/44** |

## Nearest Misses

| Symbol | Age | Liquidity | Volume | FDV | Missing |
|--------|-----|-----------|--------|-----|---------|
| CAT | 556s | $20K | $103K | $73K | liquidity ($20K < $50K) |
| assfart | 438s | $0 | $55K | $22K | liquidity ($0) |
| DREAM | 928s | $6K | $72K | $7K | liquidity ($6K) |

## Diagnosis

The board filter combination (age <=1h, liq >$50K, cap >$7K, vol >$1K, Trending 6H) is too restrictive for current market conditions. Newly created Solana tokens (<1h old) very rarely accumulate $50K+ liquidity within their first hour. The only 11 tokens with >$50K liquidity were all older than 3.5 hours.

## Task Boundary

Per task spec: "If no fresh candidates qualify, report the exact counts and stop without reusing stale candidates." — stopping here. No trades, no marks, no PnL to compare with MT-444.

## Comparison to MT-444 (N/A)

No trade data available. The candidate pool was empty, so no comparison is possible.
