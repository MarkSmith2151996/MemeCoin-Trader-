# Dune Historical Backtest Results

Run date: 2026-08-13

## Data Pull

- Query A execution `01KZXXBZ0R6VJ2E52YKGMTCBPQ` (Dune query `8312067`) completed and was paginated into `data/dune/graduated_tokens.csv`: 1,821 rows.
- Query B was not present under the supplied API key, so public Dune query `8318457` was created from the versioned `QUERY B` block in `analysis/dune_queries.sql`.
- Query B execution `01KZXY4JKNDS7DB1STN432Q467` completed successfully but returned zero rows. Its 30-day Pump.fun withdrawal filter had no matching Raydium/PumpSwap wSOL swaps under the selected decoded tables and DEX filters.
- `data/dune/token_swaps.csv` was saved with its CSV header and zero data rows.

## Observable Strategy B Replay

| Metric | Result |
| --- | ---: |
| Query A tokens loaded | 1,821 |
| Tokens passing observable gates | 718 |
| Observable gate pass rate | 39.43% |
| Mints with valid two-hour price paths | 0 |
| Closed simulated positions | 0 |
| Realized PnL at 0.05 SOL sizing | +0.000000 SOL |
| Simulated win rate | Not available |

The replay applies the observable age, market-cap, volume, transaction-count, and buy/sell gates. Query A V1 lacks an explicit supply or market cap, so its `min_price_usd` is multiplied by Pump.fun's conventional 1B token supply as a clearly labeled estimate. It also reports current Query A's 24-hour token aggregates, not the original Query A graduation-window columns.

## Paper Trading Comparison

The read-only Strategy B paper database comparison at runtime reported 662 closed positions, +3.486896 SOL realized PnL, and a 35.50% win rate.

No numerical comparison to the Dune replay is valid because Query B contains no price paths. The backtest has not inferred exits, PnL, or a win rate from summary-only Query A data.

## Limitations

- Query B's successful zero-row result must be investigated in Dune before drawing strategy conclusions. Likely review points are the Pump.fun decoded withdrawal table, post-graduation token mint linkage, and DEX/project labels.
- Dune exports cannot reconstruct historical RugCheck, holder/creator, Grok mention, UTC-hour, or repeat-loser gates; these remain explicitly unobserved.
- Dune trade data does not provide historical pool reserves. The generated analysis treats first-trade notional as a proxy only where that field is available.
