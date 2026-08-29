# V2 Creator History

This is a data-only enrichment lane. It does not add a `gate_config` row, change
selection SQL, alter exits, or send trades.

## One-Time Schema Step

Apply `migrations/0002_creator_history.sql` as the `custodian` schema owner before
starting the collector or history refresh. The running `memecoin_writer` role must
not run this DDL.

## Daily Refresh

`scripts/build_creator_history.py` reads creator and launch timestamps from
`/mnt/d/pumpapi-replay/derived/births/*.parquet`. It joins the established archive
outcomes in `results/token_outcomes.csv` and `results/extended_holdout_outcomes.csv`.
The optional `memecoin-creator-history.timer` runs it at 00:15 UTC; install and
enable that timer separately after reviewing the service file.

Each refresh creates one as-of-UTC-day snapshot. It only includes launches before
that day, so same-day launches cannot become evidence for each other. `prior_rug_rate`
is a documented archive proxy, not an on-chain rug verdict: among prior launches with
a recorded outcome, it is the share that did not reach the archive's existing 2x
outcome. Launches without an outcome increase `prior_deploy_count` but not the rate
denominator. The table records the source-through day so delayed archive output is
visible.

## Collector Review

`memecoin-data.service` remains an additive collector. For each PumpPortal
`subscribeNewToken` observation it now persists the creator wallet, raw initial token
buy, raw SOL amount, and a self-snipe percentage computed from the reported initial
buy plus virtual-token reserve. It performs one cached read of the current creator
history per creator per UTC day and copies the resulting prior deploy count/rug rate
onto the candidate. Jupiter rows remain unchanged except for nullable enrichment
columns. No service is restarted or enabled by this change; review and apply the SQL
migration before trusting these fields.
