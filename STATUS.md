# Memecoin Trader Status

Project owner: Berj
Domain: automated Solana meme coin trading
Default execution mode: paper trading

<!-- AUTO-MANAGED -->

## Key Config

- Project name: `memecoin-trader`
- Repo path: `/home/dev/projects/memecoin-trader`
- Python: `>=3.11`
- Settings file: `config/settings.yaml`
- Task prefix: `CT`

## Architecture

- `src/core/` defines shared config, database helpers, and Pydantic domain contracts.
- `src/chain/` contains Solana/Jupiter/wallet integration placeholders.
- `src/risk/` contains token risk checks and aggregate scoring, including signal-aware token enrichment for bounded paper-cycle runs.
- `src/execution/` defines the execution adapter contract plus paper and live adapter implementations.
- `src/signals/` defines async signal-source contracts; `aggregator.py` safely starts/stops/polls sources, clusters same-mint signals by time window, boosts multi-source composites, and best-effort logs ranked opportunities, while `whale_tracker.py`, `pump_fun.py`, `onchain.py`, and `twitter.py` provide Helius, PumpPortal, DexScreener, and Twitter/X-backed signal feeds.
- `src/strategy/` contains decision, portfolio, position, and exit helpers.
- `src/monitoring/` contains lightweight health, alert, and dashboard helpers for runtime visibility.
- `src/cli.py` exposes operator commands including health/config inspection and a bounded `paper-cycle` runtime that polls signal sources, routes signals through the decision engine, persists paper trades/positions, and prints a safe summary before terminating, with strict and discovery paper-only risk profiles.

## File Map

- `.env.example`: expected environment variables for RPC, APIs, wallet, and position caps.
- `pyproject.toml`: Python package metadata, dependencies, dev dependencies, pytest, and Ruff config.
- `config/settings.yaml`: risk, position, exit, execution, and monitoring defaults.
- `config/wallets_to_track.yaml`: public sample wallet watchlist used for safe whale-tracker dry runs until real tracked wallets are configured locally.
- `scripts/check_token.py`: smoke-check script for building a token model and running risk scoring.
- `src/risk/scorer.py`: aggregate risk scoring plus signal-aware `TokenInfo` enrichment from raw pump.fun/on-chain payload fields for paper-cycle diagnostics, discovery-mode age relaxation, holder-alias mapping, and read-only holder lookups with aggregate outcomes for discovery-mode concentration checks.
- `src/cli.py`: Typer entrypoint for health/config commands plus the bounded `paper-cycle` runner over existing signal, decision, and paper execution paths, including strict/discovery paper-only risk profiles and aggregate rejection diagnostics for skipped signals.
- `src/signals/aggregator.py`: independent signal aggregator that safely polls registered sources, deduplicates same-mint signals inside a configurable time window, boosts cross-source composites, ranks opportunities, and best-effort logs them to any compatible signals table or recorder.
- `src/signals/onchain.py`: DexScreener-backed read-only on-chain monitor that discovers candidate mints from token profiles, fetches Solana pair snapshots, scores volume spikes, buy/sell momentum, liquidity changes, and recent-pool activity, then deduplicates emitted mints within a five-minute poll window.
- `src/signals/pump_fun.py`: websocket-first pump.fun monitor with verified PumpPortal `subscribeNewToken` and `subscribeMigration` subscriptions plus v3 HTTP fallback over `/coins` and `/coins/currently-live`.
- `src/signals/twitter.py`: Twitter/X monitor that uses recent search when `TWITTER_BEARER_TOKEN` is present, safely degrades without configured credentials, extracts `$TICKER` and Solana mint mentions, deduplicates posts by ID, and emits deterministic velocity/account/follower-weighted `Signal` objects only when a mint address is present.
- `src/signals/whale_tracker.py`: Helius enhanced-transactions poller that reads `HELIUS_API_KEY` from environment or local `.env`, includes token-account activity, deduplicates signatures, and emits whale-buy `Signal` objects.
- `src/strategy/decision_engine.py`: async risk-gated buy evaluation plus open-position exit scanning and sell execution, with structured rejection reasons for paper-cycle diagnostics.
- `src/strategy/position_manager.py`: async open-position persistence, exposure tracking, partial exits, and close handling.
- `src/strategy/exits.py`: take-profit ladder, stop-loss, time-stop, liquidity, and emergency exit evaluation.
- `src/monitoring/health.py`: process-level health probe plus the dashboard-compatible `HealthMonitor` shim.
- `src/monitoring/dashboard.py`: Rich terminal dashboard that reads the canonical runtime DB path, recent trades/positions, and monitoring health.
- `tests/`: smoke tests for risk contracts, signal aggregation, paper execution, whale tracker polling behavior, and pump.fun normalization/deduplication.
- `tests/test_aggregator.py`: focused coverage for signal-source fan-out, same-mint clustering inside/outside the dedupe window, composite ranking, safe single-source passthrough, source-failure isolation, and optional SQLite signal logging.
- `tests/test_cli_paper_cycle.py`: bounded paper-cycle coverage for accepted/rejected fake signals, stable rejection-reason counts, paper-only enforcement, max-signal/timeout termination, SQLite persistence, and safe CLI summary output.
- `tests/test_e2e_paper.py`: offline signal-to-trade smoke covering decision-engine approval, paper execution, trade persistence, position persistence, and dashboard visibility on a temporary SQLite DB.
- `tests/test_onchain_provider.py`: focused provider coverage for `OnChainMonitor` interface conformance, deterministic DexScreener scoring helpers, graceful provider degradation, mint-level dedupe, and explicit confirmation that no wallet/trading code is involved.
- `tests/test_pump_fun_provider.py`: focused provider-shape coverage for live-style pump.fun create payloads, graduation detection from migration fields, and safe handling of websocket ack/malformed messages.
- `tests/test_twitter_provider.py`: focused provider coverage for Twitter/X payload normalization, no-key degradation, ticker/mint extraction helpers, mention-velocity scoring, and post-ID deduplication without live network calls.
- `tests/test_whale_tracker_provider.py`: focused provider tests for dotenv-based Helius key loading, token-account polling params, Helius-style buy normalization, dedupe, and safe empty/malformed handling.
- `tests/test_strategy.py`: focused coverage for decision-engine risk gating and exit-rule behavior.
- `tests/test_monitoring.py`: focused coverage for dashboard DB-path resolution, health compatibility, and one-shot dashboard rendering.

## Last 10 Changes

- 2026-07-07 Replaced the Twitter/X placeholder with a `TwitterMonitor` that uses recent search when a bearer token exists, recognizes `$TICKER` and Solana mint mentions, scores mention velocity plus account diversity and follower weight, and safely returns no signals when credentials are absent or only a Grok key is configured; `python3 -m compileall src/signals/twitter.py`, focused provider pytest, and the full pytest suite all passed.
- 2026-07-07 Built `OnChainMonitor` as a read-only DexScreener signal source that discovers candidate Solana mints from token profiles, fetches `latest/dex/tokens/{mint}` pair snapshots, scores volume spikes, buy/sell momentum, liquidity changes, and recent-pool activity with deterministic helpers, and deduplicates emitted mints within a five-minute poll window; `python3 -m compileall src/signals/onchain.py`, focused provider pytest, and the full pytest suite all passed.
- 2026-07-07 Built `SignalAggregator` as an independent signal fan-out layer that safely starts/stops/polls async sources, clusters same-mint signals inside a configurable dedupe window, boosts multi-source composites, ranks opportunities by composite score, and best-effort logs to a compatible `signals` table or recorder; `python3 -m compileall src/signals/aggregator.py`, targeted aggregator pytest, and the full pytest suite all passed.
- 2026-07-07 Added a read-only holder lookup path for discovery-mode paper-cycle scoring that uses Solana/Helius RPC token supply plus largest-account reads to compute `top10_holder_pct` when payload fields are absent; focused tests are green, the latest real discovery smoke now reports `top10_holder_check_failed=3` and `top10_holder_check_unknown=2` with aggregate holder lookup outcomes `holder_lookup_threshold_failed=3` and `holder_lookup_failed_provider=2`, and the remaining full-suite failures are unrelated `tests/test_onchain_provider.py` regressions outside this task's file scope.
- 2026-07-07 Extended holder-concentration alias mapping for discovery-mode paper-cycle scoring so payload fields like `top10HolderPercent`, `creatorPercent`, and `totalHolders` populate `TokenInfo` when present; targeted and full pytest are green, but the latest real discovery smoke still reported `top10_holder_check_unknown=5` because current live pump.fun payloads did not include holder concentration fields.
- 2026-07-07 Added a paper-only `discovery` risk profile for `paper-cycle` that relaxes only the age gate while keeping strict mode as the default; targeted and full pytest are green, and the latest real strict run reported `age_check_failed=5` while the matching discovery run advanced to `top10_holder_check_unknown=5` with 0 accepted buys in both modes.
- 2026-07-07 Added signal-aware pump.fun/on-chain token enrichment for bounded paper-cycle risk scoring so raw signal payloads now populate `TokenInfo` fields like `liquidity_sol`, `created_at`, and authority flags before risk evaluation; targeted and full pytest are green, and the latest real paper-mode smoke still approved 0 buys but advanced rejection diagnostics from `liquidity_check_unknown=5` to `age_check_failed=5`.
- 2026-07-07 Added aggregate paper-cycle rejection diagnostics so bounded runs now report stable labels like `liquidity_check_unknown`, `honeypot_check_failed`, and `position_size_zero`; targeted and full pytest are green, and the latest real paper-mode smoke collected 5 signals, approved 0 buys, and reported `liquidity_check_unknown=5` with no persisted trades/positions.
- 2026-07-07 Fixed `DecisionEngine` callable risk-scorer fallback so the bounded `paper-cycle` runtime can retry plain callables like `assess_token` with `TokenInfo` after a failed `Signal` probe; reran targeted tests plus the full pytest suite, then verified a real paper-mode smoke collected 5 signals, approved 0 buys, and persisted 0 trades/positions without dashboard warnings.
- 2026-07-07 Added a bounded `python3 -m src.cli paper-cycle --max-signals N --timeout-seconds T` runtime that polls existing signal sources safely, forces paper execution, persists trades/positions to SQLite, prints only a concise summary, and terminates on `max_signals` or timeout; added focused CLI/runtime tests and reran the full pytest suite successfully.
- 2026-07-06 Ad hoc Helius retry: Fixed `WhaleWalletTracker` so an explicit empty `api_key` no longer falls back to local `.env`, reran the focused whale-tracker test and full pytest suite successfully, and verified a bounded one-wallet Helius paper-mode dry run authenticates and returns data safely.

## Known Issues

- Live Jupiter execution is intentionally not implemented yet.
- Whale tracker is polling-only for now; webhook ingestion still needs a public callback endpoint and receiver.
- pump.fun websocket token-creation and migration subscriptions are now verified against PumpPortal, but the short dry-run did not capture a live migration payload; graduation normalization is still proven by offline fixtures rather than observed runtime traffic.
- Risk checks use conservative local token fields until on-chain enrichment is implemented.
