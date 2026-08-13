# Dune Historical Backtest

`scripts/dune_backtest.py` is a detached, read-only replay of observable Strategy B gates and exits over Dune exports. It does not import the runtime loop, write to `data/trades.db`, or alter any strategy parameter.

## Dune Queries

1. Open [Dune's new-query editor](https://dune.com/queries/new).
2. Open `analysis/dune_queries.sql` locally and paste the `QUERY A` block into the editor.
3. Run it, save it as `MT-533 graduated tokens`, and export the result with `Download CSV`. Save it as `data/dune/graduated_tokens.csv`.
4. Open the same [new-query editor](https://dune.com/queries/new), paste the `QUERY B` block, run it, save it as `MT-533 graduated token swaps`, and export it to `data/dune/graduated_token_swaps.csv`.

Query A asks Dune for Pump.fun withdrawal/graduation records with a Raydium or PumpSwap wSOL market during the last 30 days. Query B exports every qualifying token's first two hours of wSOL swap prices.

## Dune Schema Check

The SQL uses Dune's decoded Pump.fun tables `pumpdotfun_solana.pump_call_create` and `pumpdotfun_solana.pump_call_withdraw`, plus `dex_solana.trades`. Dune occasionally renames decoded-program schemas. If the editor reports one of the Pump.fun tables missing, use the Data Explorer to find the current decoded Pump.fun `create` and `withdraw` tables, replace just those two source names, and preserve the aliases `account_mint` and `call_block_time` used by the queries.

`market_cap_usd_at_graduation` is a transparent estimate: the first observed post-graduation USD price times the conventional 1 billion Pump.fun token supply. Dune trade rows do not provide a historical pool-reserve or verified circulating-supply snapshot. Likewise, `liquidity_added_usd_proxy` is the first observed trade notional, not a true liquidity reserve.

## Run

From the repository root:

```bash
python3 scripts/dune_backtest.py
```

The expected inputs are:

```text
data/dune/graduated_tokens.csv
data/dune/graduated_token_swaps.csv
```

To use differently named exports or keep output elsewhere:

```bash
python3 scripts/dune_backtest.py \
  --graduations /path/to/query-a.csv \
  --swaps /path/to/query-b.csv \
  --output-dir analysis/dune_backtest_output
```

Results are written to `analysis/dune_backtest_output/`:

- `per_trade_results.csv`: every graduation, observable gate results, and simulated entry/exit row for gate passes.
- `summary.json`: total closed-within-two-hours PnL at Strategy B's 0.05 SOL paper size, win rate, exit reasons, and a read-only comparison with closed Strategy B paper positions in `data/trades.db`.

## Interpretation Boundary

This pipeline applies the observable age, market-cap, first-30-minute volume, transaction-count, and buy/sell-ratio gates. It cannot recreate live-only RugCheck, holder/creator, Grok mention, UTC-hour, or repeat-loser filters from historical Dune swaps, and it records those as unobservable rather than silently assuming they passed. Entries are the first recorded post-graduation wSOL swap. Any path still open at the two-hour export boundary is separated from realized PnL as `open_at_end`.
