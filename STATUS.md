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
- `src/risk/` contains token risk checks and aggregate scoring.
- `src/execution/` defines the execution adapter contract plus paper and live adapter implementations.
- `src/signals/` defines async signal-source contracts; `whale_tracker.py` polls Helius enhanced address transactions and `pump_fun.py` now buffers websocket events, falls back to HTTP polling, and normalizes pump.fun payloads into `Signal` objects.
- `src/strategy/` contains decision, portfolio, position, and exit helpers.
- `src/monitoring/` contains lightweight health, alert, and dashboard helpers for runtime visibility.

## File Map

- `.env.example`: expected environment variables for RPC, APIs, wallet, and position caps.
- `pyproject.toml`: Python package metadata, dependencies, dev dependencies, pytest, and Ruff config.
- `config/settings.yaml`: risk, position, exit, execution, and monitoring defaults.
- `config/wallets_to_track.yaml`: public sample wallet watchlist used for safe whale-tracker dry runs until real tracked wallets are configured locally.
- `scripts/check_token.py`: smoke-check script for building a token model and running risk scoring.
- `src/signals/pump_fun.py`: websocket-first pump.fun monitor with verified PumpPortal `subscribeNewToken` and `subscribeMigration` subscriptions plus v3 HTTP fallback over `/coins` and `/coins/currently-live`.
- `src/signals/whale_tracker.py`: Helius enhanced-transactions poller that reads `HELIUS_API_KEY` from environment or local `.env`, includes token-account activity, deduplicates signatures, and emits whale-buy `Signal` objects.
- `src/strategy/decision_engine.py`: async risk-gated buy evaluation plus open-position exit scanning and sell execution.
- `src/strategy/position_manager.py`: async open-position persistence, exposure tracking, partial exits, and close handling.
- `src/strategy/exits.py`: take-profit ladder, stop-loss, time-stop, liquidity, and emergency exit evaluation.
- `src/monitoring/health.py`: process-level health probe plus the dashboard-compatible `HealthMonitor` shim.
- `src/monitoring/dashboard.py`: Rich terminal dashboard that reads the canonical runtime DB path, recent trades/positions, and monitoring health.
- `tests/`: smoke tests for risk contracts, signal aggregation, paper execution, whale tracker polling behavior, and pump.fun normalization/deduplication.
- `tests/test_e2e_paper.py`: offline signal-to-trade smoke covering decision-engine approval, paper execution, trade persistence, position persistence, and dashboard visibility on a temporary SQLite DB.
- `tests/test_pump_fun_provider.py`: focused provider-shape coverage for live-style pump.fun create payloads, graduation detection from migration fields, and safe handling of websocket ack/malformed messages.
- `tests/test_whale_tracker_provider.py`: focused provider tests for dotenv-based Helius key loading, token-account polling params, Helius-style buy normalization, dedupe, and safe empty/malformed handling.
- `tests/test_strategy.py`: focused coverage for decision-engine risk gating and exit-rule behavior.
- `tests/test_monitoring.py`: focused coverage for dashboard DB-path resolution, health compatibility, and one-shot dashboard rendering.

## Last 10 Changes

- 2026-07-06 Ad hoc Helius retry: Fixed `WhaleWalletTracker` so an explicit empty `api_key` no longer falls back to local `.env`, reran the focused whale-tracker test and full pytest suite successfully, and verified a bounded one-wallet Helius paper-mode dry run authenticates and returns data safely.
- 2026-07-06 CT-094: Committed the pending pump.fun provider verification changes, pushed `master` to the new GitHub `origin`, and kept the local `opencode.json` out of git because it contains an API key; Berj access was added via `/home/dev/bin/berj-picker`.
- 2026-07-06 CT-093: Added dotenv-backed Helius key loading, switched whale polling to include token-account balance changes, replaced placeholder wallets with public sample addresses, and added focused provider tests; live Helius verification failed locally because `.env` and `HELIUS_API_KEY` were still missing at execution time.
- 2026-07-06 CT-091: Verified PumpPortal websocket token-creation and migration subscriptions live, corrected the rejected `subscribeNewPairs` assumption, switched HTTP fallback to working frontend v3 coin-list endpoints, and added focused provider-shape tests.
- 2026-07-06 CT-090: Added an offline end-to-end paper smoke test that pushes a fake pump.fun signal through the real decision engine, persists the trade and position to a temporary DB, and verifies dashboard visibility without external APIs.
- 2026-07-06 CT-088: Added a dashboard-compatible `HealthMonitor`, aligned the dashboard's canonical DB path to `data/trades.db`, and tightened monitoring smoke coverage.
- 2026-07-06 CT-082: Implemented the decision engine, position manager, exit ladder, and focused strategy tests.
- 2026-07-06 CT-080: Implemented the pump.fun monitor with websocket buffering, HTTP fallback polling, payload normalization, and focused signal tests.
- 2026-07-06 CT-081: Implemented a Helius polling-based whale wallet tracker, added tracked-wallet config placeholders, and covered signal emission/dedup behavior with focused tests.
- 2026-07-02 CT-079: Created Phase 1 scaffold and project metadata for Memecoin Trader.

## Known Issues

- Live Jupiter execution is intentionally not implemented yet.
- Whale tracker is polling-only for now; webhook ingestion still needs a public callback endpoint and receiver.
- pump.fun websocket token-creation and migration subscriptions are now verified against PumpPortal, but the short dry-run did not capture a live migration payload; graduation normalization is still proven by offline fixtures rather than observed runtime traffic.
- Risk checks use conservative local token fields until on-chain enrichment is implemented.
