# Dune Historical Backtest Results

Run date: 2026-08-13

## Data Pull

- Query A execution `01KZXXBZ0R6VJ2E52YKGMTCBPQ` (Dune query `8312067`) completed and was paginated into `data/dune/graduated_tokens.csv`: 1,821 rows.
- Dune Query B `8318399`, execution `01KZXXR60M1WN2VNBMWA4VQWQS`, completed successfully with 500,000 rows (the API result-set limit). All 500 pages were fetched in 1,000-row increments and combined into ignored `data/dune/token_swaps.csv` with one header row.
- The live result schema is `mint_address`, `timestamp`, `price_usd`, `token_amount`, and `amount_usd`. The detached parser now accepts this UTC/ USD schema; exit logic is ratio-based, so price denomination does not alter percentage exits or fixed 0.05-SOL percentage PnL.

## Observable Strategy B Replay

| Metric | Result |
| --- | ---: |
| Query A tokens loaded | 1,821 |
| Tokens passing observable gates | 718 |
| Observable gate pass rate | 39.43% |
| Mints with valid post-graduation price paths | 9 |
| Closed simulated positions | 7 |
| Open at two-hour export cutoff | 2 |
| Realized PnL at 0.05 SOL sizing | -0.021433 SOL |
| Simulated win rate | 0.00% (0/7) |
| Per-trade Sharpe | -1.561 (unannualized) |
| Exits | 3 trailing stops, 3 hard stops, 1 early no-green |

The replay applies the observable age, market-cap, volume, transaction-count, and buy/sell gates. Query A V1 lacks an explicit supply or market cap, so its `min_price_usd` is multiplied by Pump.fun's conventional 1B token supply as a clearly labeled estimate. It also reports current Query A's 24-hour token aggregates, not the original Query A graduation-window columns. Entries use only the first swap at or after Query A's graduation timestamp and discard swaps after the two-hour horizon, preventing pre-signal prices from being replayed as entries.

## Paper Trading Comparison

The read-only Strategy B paper database comparison at runtime reported 662 closed positions, +3.486896 SOL realized PnL, and a 35.50% win rate.

The runtime comparison is descriptive only: Dune cannot reconstruct historical RugCheck, holder/creator, Grok, UTC-hour, or repeat-loser gates. The 7-trade Dune sample is too small and its observable-only gate set differs from paper runtime behavior, so it is not sufficient to change Strategy B parameters.

## Limitations

- Dune exports cannot reconstruct historical RugCheck, holder/creator, Grok mention, UTC-hour, or repeat-loser gates; these remain explicitly unobserved.
- Dune trade data does not provide historical pool reserves. The generated analysis treats first-trade notional as a proxy only where that field is available.
- Query B was capped at Dune's 500,000-row API result limit and covers only 63 mints, 9 of which pass the observable gates and have post-graduation price paths. It is not a complete population-level result.
- Sharpe is an unannualized mean divided by sample standard deviation of the seven closed per-trade SOL PnLs; it is descriptive rather than a time-series annualized Sharpe.
