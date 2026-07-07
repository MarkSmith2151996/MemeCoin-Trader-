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
- `src/chain/` contains Solana/Jupiter/wallet integration placeholders plus a pre-live Jito block-engine scaffold for MEV-aware bundle request building and injectable submission testing.
- `src/risk/` contains token risk checks, aggregate scoring, standalone buyer funding-source analysis for sybil/bundle detection, including signal-aware token enrichment for bounded paper-cycle runs and a standalone RugCheck client that can normalize external safety reports for later scorer integration.
- `src/execution/` defines the execution adapter contract plus paper and live adapter implementations, with a disabled-by-default live Jupiter submission helper that can optionally route serialized swaps through Jito and fall back to RPC via injected submitters while leaving real live trading disabled.
- `src/signals/` defines async signal-source contracts; `aggregator.py` safely starts/stops/polls sources, clusters same-mint signals by time window, boosts multi-source composites, and best-effort logs ranked opportunities, while `whale_tracker.py`, `pump_fun.py`, `onchain.py`, and `twitter.py` provide Helius, PumpPortal, DexScreener, and Twitter/X-backed signal feeds.
- `src/strategy/` contains decision, portfolio, position, fixed-exit helpers, and standalone dynamic-exit calibration helpers that are not yet wired into runtime behavior.
- `src/monitoring/` contains lightweight health, alert, and dashboard helpers for runtime visibility.
- `src/cli.py` exposes operator commands including health/config inspection and a bounded `paper-cycle` runtime that evaluates ranked aggregated opportunities from pump.fun, whale, on-chain, and Twitter sources, persists paper trades/positions, and prints a safe summary before terminating, with strict and discovery paper-only risk profiles.

## File Map

- `.env.example`: expected environment variables for RPC, APIs, wallet, and position caps.
- `pyproject.toml`: Python package metadata, dependencies, dev dependencies, pytest, and Ruff config.
- `config/settings.yaml`: risk, position, exit, execution, and monitoring defaults.
- `config/wallets_to_track.yaml`: public sample wallet watchlist used for safe whale-tracker dry runs until real tracked wallets are configured locally.
- `scripts/check_token.py`: smoke-check script for building a token model and running risk scoring.
- `src/chain/jito.py`: pre-live Jito block-engine scaffold that normalizes serialized transactions into bundle payloads, carries optional tip metadata, submits through an injectable HTTP client, and returns structured non-throwing results for success and degradation paths.
- `src/execution/jupiter_live.py`: live Jupiter adapter boundary that still blocks direct live swaps, but now exposes an injected `submit_serialized_swap()` helper with explicit Jito gating, structured diagnostics (`jito_disabled`, `jito_attempted`, `jito_bundle_submitted`, `jito_failed_fallback_rpc`, `jito_failed_no_fallback`), and safe RPC fallback without logging raw transaction payloads.
- `src/risk/rugcheck.py`: read-only RugCheck client for `v1/tokens/{mint}/report` that normalizes authority flags, top-holder concentration, liquidity state, honeypot indicator, and provider risk score/level while degrading safely on provider failures or malformed responses.
- `src/risk/funding_provider.py`: Helius-backed inbound SOL funding adapter that resolves `HELIUS_API_KEY` from env or local `.env`, fetches recent enhanced transactions through an injectable HTTP layer, normalizes only inbound native SOL transfers into `InboundTransfer` records, and degrades safely on missing credentials, non-200 responses, timeouts, and malformed payloads.
- `src/risk/funding_analysis.py`: async funding-source analyzer that groups buyer wallets by recent inbound funder, computes bundled-buyer concentration metrics, and flags likely sybil or bundled launches while degrading gracefully when provider data is missing.
- `src/risk/scorer.py`: aggregate risk scoring plus signal-aware `TokenInfo` enrichment from raw pump.fun/on-chain payload fields for paper-cycle diagnostics, discovery-mode age relaxation, holder-alias mapping, and read-only holder lookups with aggregate outcomes for discovery-mode concentration checks.
- `src/cli.py`: Typer entrypoint for health/config commands plus the bounded `paper-cycle` runner over integrated aggregated signal sources, including strict/discovery paper-only risk profiles and aggregate source/rejection diagnostics for skipped opportunities.
- `src/signals/aggregator.py`: independent signal aggregator that safely polls registered sources, deduplicates same-mint signals inside a configurable time window, boosts cross-source composites, ranks opportunities, and best-effort logs them to any compatible signals table or recorder.
- `src/signals/onchain.py`: DexScreener-backed read-only on-chain monitor that discovers candidate mints from token profiles, fetches Solana pair snapshots, scores volume spikes, buy/sell momentum, liquidity changes, and recent-pool activity, then deduplicates emitted mints within a five-minute poll window.
- `src/signals/pump_fun.py`: websocket-first pump.fun monitor with verified PumpPortal `subscribeNewToken` and `subscribeMigration` subscriptions plus v3 HTTP fallback over `/coins` and `/coins/currently-live`.
- `src/signals/twitter.py`: Twitter/X monitor that uses recent search when `TWITTER_BEARER_TOKEN` is present, safely degrades without configured credentials, extracts `$TICKER` and Solana mint mentions, deduplicates posts by ID, and emits deterministic velocity/account/follower-weighted `Signal` objects only when a mint address is present.
- `src/signals/whale_tracker.py`: Helius enhanced-transactions poller that reads `HELIUS_API_KEY` from environment or local `.env`, includes token-account activity, deduplicates signatures, and emits whale-buy `Signal` objects.
- `src/strategy/decision_engine.py`: async risk-gated buy evaluation plus open-position exit scanning and sell execution, with structured rejection reasons for paper-cycle diagnostics.
- `src/strategy/dynamic_exits.py`: helper-only dynamic exit calibration module with deterministic checks for volume-decay exits, trail-start eligibility, liquidity-drop emergencies, and aggregate reason-label summaries, intentionally not yet wired into active exit management.
- `src/strategy/position_manager.py`: async open-position persistence, exposure tracking, partial exits, and close handling.
- `src/strategy/exits.py`: take-profit ladder, stop-loss, time-stop, liquidity, and emergency exit evaluation.
- `src/monitoring/health.py`: process-level health probe plus the dashboard-compatible `HealthMonitor` shim.
- `src/monitoring/dashboard.py`: Rich terminal dashboard that reads the canonical runtime DB path, recent trades/positions, and monitoring health.
- `tests/`: smoke tests for risk contracts, signal aggregation, paper execution, whale tracker polling behavior, and pump.fun normalization/deduplication.
- `tests/test_jito.py`: focused coverage for Jito bundle payload construction, bundle-id parsing, graceful degradation on non-200, timeout, malformed JSON, explicit confirmation that no wallet/private-key or real network call is required, plus disabled-by-default live-adapter integration and Jito-to-RPC fallback behavior.
- `tests/test_aggregator.py`: focused coverage for signal-source fan-out, same-mint clustering inside/outside the dedupe window, composite ranking, safe single-source passthrough, source-failure isolation, and optional SQLite signal logging.
- `tests/test_cli_paper_cycle.py`: bounded paper-cycle coverage for accepted/rejected fake signals, aggregator integration, composite opportunities, non-fatal source degradation, paper-only enforcement, max-signal/timeout termination, SQLite persistence, and safe CLI summary output.
- `tests/test_dynamic_exits.py`: focused helper coverage for volume-decay threshold timing, trail-start multiple calibration, liquidity-drop emergency thresholds, aggregate reason labels, and import-only safety with no runtime exit behavior changes.
- `tests/test_e2e_paper.py`: offline signal-to-trade smoke covering decision-engine approval, paper execution, trade persistence, position persistence, and dashboard visibility on a temporary SQLite DB.
- `tests/test_funding_analysis.py`: focused coverage for bundled-buyer detection thresholds, diverse and unknown funding outcomes, empty input neutrality, provider-failure degradation, and timestamp window filtering without live network calls.
- `tests/test_funding_provider.py`: focused provider coverage for Helius inbound SOL normalization, missing-key and HTTP/provider failure degradation, malformed JSON handling, irrelevant transfer filtering, and explicit confirmation that fake fetchers prevent live network calls.
- `tests/test_onchain_provider.py`: focused provider coverage for `OnChainMonitor` interface conformance, deterministic DexScreener scoring helpers, graceful provider degradation, mint-level dedupe, and explicit confirmation that no wallet/trading code is involved.
- `tests/test_pump_fun_provider.py`: focused provider-shape coverage for live-style pump.fun create payloads, graduation detection from migration fields, and safe handling of websocket ack/malformed messages.
- `tests/test_rugcheck.py`: focused client coverage for successful RugCheck normalization plus graceful handling of missing fields, non-200 responses, timeouts/provider errors, malformed JSON, and non-object payloads.
- `tests/test_twitter_provider.py`: focused provider coverage for Twitter/X payload normalization, no-key degradation, ticker/mint extraction helpers, mention-velocity scoring, and post-ID deduplication without live network calls.
- `tests/test_whale_tracker_provider.py`: focused provider tests for dotenv-based Helius key loading, token-account polling params, Helius-style buy normalization, dedupe, and safe empty/malformed handling.
- `tests/test_strategy.py`: focused coverage for decision-engine risk gating and exit-rule behavior.
- `tests/test_monitoring.py`: focused coverage for dashboard DB-path resolution, health compatibility, and one-shot dashboard rendering.

## Last 10 Changes

- 2026-07-07 Wired the pre-live Jito scaffold into `src/execution/jupiter_live.py` through an injected serialized-submission helper that keeps direct live swaps disabled, attempts Jito only when explicitly enabled, falls back to RPC when allowed, and emits structured diagnostics for disabled/attempted/submitted/fallback/no-fallback paths; focused Jito pytest and full `python3 -m pytest -q` validation pending in this task execution.

- 2026-07-07 Added `src/risk/funding_provider.py` as a Helius-backed inbound SOL funding adapter for buyer-funder analysis, with env/`.env` API-key resolution, injectable enhanced-transaction fetching, normalization into `InboundTransfer` records, and safe degradation on missing credentials, timeout, non-200, malformed JSON, and irrelevant transfer paths; `python3 -m compileall src/risk/funding_provider.py` and focused funding-provider/funding-analysis pytest passed, while the full `python3 -m pytest -q` suite still hit unrelated discovery-holder diagnostic failures in `tests/test_cli_paper_cycle.py` and `tests/test_risk.py` outside this task's file scope.
- 2026-07-07 Integrated `SignalAggregator`, `OnChainMonitor`, and `TwitterMonitor` into bounded `paper-cycle`, so strict and discovery runs now evaluate ranked multi-source opportunities from pump.fun, whale, on-chain, and Twitter feeds with aggregate source diagnostics; `python3 -m compileall src`, focused CLI/aggregator/on-chain/Twitter pytest, and full `python3 -m pytest -q` all passed. Latest strict smoke collected 5 aggregated opportunities and rejected all 5 on `liquidity_check_unknown`, while the latest discovery smoke evaluated 20 aggregated opportunities, produced 0 paper trades, and hit `top10_holder_check_failed=18`, `liquidity_check_unknown=2` with holder lookup outcomes `holder_lookup_threshold_failed=18`, `holder_lookup_succeeded=1`, and `holder_lookup_failed_provider=1`.
- 2026-07-07 Added `src/risk/funding_analysis.py` as a provider-injected buyer funding analyzer that groups recent inbound SOL funders, reports bundled-buyer concentration metrics, flags common-funder majorities above 50%, and degrades to aggregate unknowns on missing provider data; `python3 -m compileall src/risk/funding_analysis.py`, focused funding-analysis pytest, and the full pytest suite all passed.
- 2026-07-07 Added a pre-live `JitoBlockEngineClient` scaffold that builds bundle-submission payloads from serialized transaction bytes/strings, supports configurable endpoints plus optional validator tip metadata, submits through an injectable HTTP client, and degrades into structured result objects on timeout, non-200, malformed JSON, or provider exceptions; `python3 -m compileall src/chain/jito.py`, focused Jito pytest, and the full pytest suite all passed.
- 2026-07-07 Added `src/strategy/dynamic_exits.py` as a helper-only calibration module for future exit tuning, with deterministic checks for 20%-of-peak volume decay over 15 minutes, configurable trail-start multiples defaulting to `3.0`, 50% liquidity-drop emergencies inside 60 seconds, and aggregate reason-label summaries; `python3 -m compileall src/strategy/dynamic_exits.py`, focused helper pytest, and the full pytest suite all passed.
- 2026-07-07 Added a standalone `RugCheckClient` that fetches `https://api.rugcheck.xyz/v1/tokens/{mint}/report`, normalizes authority flags, top-holder concentration, liquidity state, honeypot indicator, and provider risk score/level into a small result object, and degrades safely across timeout, non-200, malformed JSON, and malformed payload paths; `python3 -m compileall src/risk/rugcheck.py`, focused RugCheck pytest, and the full pytest suite all passed.
- 2026-07-07 Replaced the Twitter/X placeholder with a `TwitterMonitor` that uses recent search when a bearer token exists, recognizes `$TICKER` and Solana mint mentions, scores mention velocity plus account diversity and follower weight, and safely returns no signals when credentials are absent or only a Grok key is configured; `python3 -m compileall src/signals/twitter.py`, focused provider pytest, and the full pytest suite all passed.
- 2026-07-07 Built `OnChainMonitor` as a read-only DexScreener signal source that discovers candidate Solana mints from token profiles, fetches `latest/dex/tokens/{mint}` pair snapshots, scores volume spikes, buy/sell momentum, liquidity changes, and recent-pool activity with deterministic helpers, and deduplicates emitted mints within a five-minute poll window; `python3 -m compileall src/signals/onchain.py`, focused provider pytest, and the full pytest suite all passed.
- 2026-07-07 Built `SignalAggregator` as an independent signal fan-out layer that safely starts/stops/polls async sources, clusters same-mint signals inside a configurable dedupe window, boosts multi-source composites, ranks opportunities by composite score, and best-effort logs to a compatible `signals` table or recorder; `python3 -m compileall src/signals/aggregator.py`, targeted aggregator pytest, and the full pytest suite all passed.
- 2026-07-07 Added a read-only holder lookup path for discovery-mode paper-cycle scoring that uses Solana/Helius RPC token supply plus largest-account reads to compute `top10_holder_pct` when payload fields are absent; focused tests are green, the latest real discovery smoke now reports `top10_holder_check_failed=3` and `top10_holder_check_unknown=2` with aggregate holder lookup outcomes `holder_lookup_threshold_failed=3` and `holder_lookup_failed_provider=2`, and the remaining full-suite failures are unrelated `tests/test_onchain_provider.py` regressions outside this task's file scope.

## Known Issues

- Live Jupiter execution is still intentionally disabled for direct swaps; the new serialized submission helper only wires optional Jito/RPC routing for future guarded use and does not enable trading by itself.
- Whale tracker is polling-only for now; webhook ingestion still needs a public callback endpoint and receiver.
- pump.fun websocket token-creation and migration subscriptions are now verified against PumpPortal, but the short dry-run did not capture a live migration payload; graduation normalization is still proven by offline fixtures rather than observed runtime traffic.
- Risk checks use conservative local token fields until on-chain enrichment is implemented.
- Latest integrated paper-cycle runs still produced 0 paper trades. Current next blockers are `liquidity_check_unknown` in strict mode and predominantly `top10_holder_check_failed` in discovery mode after holder enrichment/lookup, with a smaller remainder still missing liquidity metadata.
