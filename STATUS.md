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
- `src/risk/` contains token risk checks, aggregate scoring, standalone buyer funding-source analysis for sybil/bundle detection, signal-aware token enrichment for bounded paper-cycle runs, and a RugCheck-assisted scorer path that can pre-populate authority, holder-concentration, and honeypot outcomes while falling back safely when provider data is missing.
- `src/execution/` defines the execution adapter contract plus paper and live adapter implementations, with a disabled-by-default live Jupiter submission helper that can optionally route serialized swaps through Jito and fall back to RPC via injected submitters while leaving real live trading disabled.
- `src/signals/` defines async signal-source contracts; `aggregator.py` safely starts/stops/polls sources, clusters same-mint signals by time window, boosts multi-source composites, and best-effort logs ranked opportunities, while `whale_tracker.py`, `pump_fun.py`, `onchain.py`, and `twitter.py` provide Helius, PumpPortal, DexScreener, and Twitter/X-backed signal feeds.
- `src/strategy/` contains decision, portfolio, position, fixed-exit helpers, and gated dynamic-exit runtime hooks for calibrated volume-decay exits, liquidity-emergency exits, and optional early trail-start signaling.
- `src/monitoring/` contains lightweight health, alert, and dashboard helpers for runtime visibility.
- `src/cli.py` exposes operator commands including health/config inspection and a bounded `paper-cycle` runtime that evaluates ranked aggregated opportunities from pump.fun, whale, on-chain, and Twitter sources, persists paper trades/positions, and prints a safe summary before terminating, with strict and discovery paper-only risk profiles.

## File Map

- `.env.example`: expected environment variables for RPC, APIs, wallet, and position caps.
- `pyproject.toml`: Python package metadata, dependencies, dev dependencies, pytest, and Ruff config.
- `config/settings.yaml`: risk, position, exit, execution, and monitoring defaults, including a default-off dynamic exit gate plus calibration knobs.
- `config/wallets_to_track.yaml`: public sample wallet watchlist used for safe whale-tracker dry runs until real tracked wallets are configured locally.
- `scripts/check_token.py`: smoke-check script for building a token model and running risk scoring.
- `src/chain/jito.py`: pre-live Jito block-engine scaffold that normalizes serialized transactions into bundle payloads, carries optional tip metadata, submits through an injectable HTTP client, and returns structured non-throwing results for success and degradation paths.
- `src/execution/jupiter_live.py`: live Jupiter adapter boundary that still blocks direct live swaps, but now exposes an injected `submit_serialized_swap()` helper with explicit Jito gating, structured diagnostics (`jito_disabled`, `jito_attempted`, `jito_bundle_submitted`, `jito_failed_fallback_rpc`, `jito_failed_no_fallback`), and safe RPC fallback without logging raw transaction payloads.
- `src/risk/rugcheck.py`: read-only RugCheck client for `v1/tokens/{mint}/report` that normalizes authority flags, top-holder concentration, liquidity state, honeypot indicator, and provider risk score/level while degrading safely on provider failures or malformed responses.
- `src/risk/funding_provider.py`: Helius-backed inbound SOL funding adapter that resolves `HELIUS_API_KEY` from env or local `.env`, fetches recent enhanced transactions through an injectable HTTP layer, normalizes only inbound native SOL transfers into `InboundTransfer` records, and degrades safely on missing credentials, non-200 responses, timeouts, and malformed payloads.
- `src/risk/funding_analysis.py`: async funding-source analyzer that groups buyer wallets by recent inbound funder, computes bundled-buyer concentration metrics, and flags likely sybil or bundled launches while degrading gracefully when provider data is missing.
- `src/risk/scorer.py`: aggregate risk scoring plus signal-aware `TokenInfo` enrichment from raw pump.fun/on-chain payload fields, optional RugCheck enrichment for authority/top-holder/honeypot fields with aggregate diagnostics, discovery-mode age relaxation, and read-only holder lookups that only backfill concentration data when RugCheck does not already provide it.
- `src/cli.py`: Typer entrypoint for health/config commands plus the bounded `paper-cycle` runner over integrated aggregated signal sources, including strict/discovery paper-only risk profiles and aggregate source/rejection diagnostics for skipped opportunities.
- `src/signals/aggregator.py`: independent signal aggregator that safely polls registered sources, deduplicates same-mint signals inside a configurable time window, boosts cross-source composites, ranks opportunities, and best-effort logs them to any compatible signals table or recorder.
- `src/signals/onchain.py`: DexScreener-backed read-only on-chain monitor that discovers candidate mints from token profiles, fetches Solana pair snapshots, scores volume spikes, buy/sell momentum, liquidity changes, and recent-pool activity, then deduplicates emitted mints within a five-minute poll window.
- `src/signals/pump_fun.py`: websocket-first pump.fun monitor with verified PumpPortal `subscribeNewToken` and `subscribeMigration` subscriptions plus v3 HTTP fallback over `/coins` and `/coins/currently-live`.
- `src/signals/twitter.py`: Twitter/X monitor that uses recent search when `TWITTER_BEARER_TOKEN` is present, safely degrades without configured credentials, extracts `$TICKER` and Solana mint mentions, deduplicates posts by ID, and emits deterministic velocity/account/follower-weighted `Signal` objects only when a mint address is present.
- `src/signals/whale_tracker.py`: Helius enhanced-transactions poller that reads `HELIUS_API_KEY` from environment or local `.env`, includes token-account activity, deduplicates signatures, and emits whale-buy `Signal` objects.
- `src/strategy/decision_engine.py`: async risk-gated buy evaluation plus open-position exit scanning and sell execution, with structured rejection reasons for paper-cycle diagnostics and opt-in liquidity-aware position caps that preserve flat sizing by default.
- `src/strategy/dynamic_exits.py`: helper-only dynamic exit calibration module with deterministic checks for volume-decay exits, trail-start eligibility, liquidity-drop emergencies, and aggregate reason-label summaries used by the exit runtime when enabled.
- `src/strategy/position_sizing.py`: helper-only liquidity tier mapping that classifies pool liquidity into conservative max-position caps and skip conditions for decision-engine use.
- `src/strategy/position_manager.py`: async open-position persistence, exposure tracking, partial exits, and close handling.
- `src/strategy/exits.py`: take-profit ladder, stop-loss, time-stop, liquidity, emergency exit evaluation, and a default-off `DynamicExitState` hook for calibrated dynamic-exit checks with testable reason labels.
- `src/monitoring/health.py`: process-level health probe plus the dashboard-compatible `HealthMonitor` shim.
- `src/monitoring/dashboard.py`: Rich terminal dashboard that reads the canonical runtime DB path, recent trades/positions, and monitoring health.
- `tests/`: smoke tests for risk contracts, signal aggregation, paper execution, whale tracker polling behavior, and pump.fun normalization/deduplication.
- `tests/test_jito.py`: focused coverage for Jito bundle payload construction, bundle-id parsing, graceful degradation on non-200, timeout, malformed JSON, explicit confirmation that no wallet/private-key or real network call is required, plus disabled-by-default live-adapter integration and Jito-to-RPC fallback behavior.
- `tests/test_aggregator.py`: focused coverage for signal-source fan-out, same-mint clustering inside/outside the dedupe window, composite ranking, safe single-source passthrough, source-failure isolation, and optional SQLite signal logging.
- `tests/test_cli_paper_cycle.py`: bounded paper-cycle coverage for accepted/rejected fake signals, aggregator integration, composite opportunities, non-fatal source degradation, paper-only enforcement, max-signal/timeout termination, SQLite persistence, and safe CLI summary output.
- `tests/test_dynamic_exits.py`: focused helper and runtime-gate coverage for volume-decay timing, liquidity emergency calibration, early trail-start signaling, aggregate reason labels, and preservation of existing behavior while dynamic exits stay disabled by default.
- `tests/test_position_sizing.py`: focused helper coverage for liquidity tier matching, unknown/invalid liquidity skips, and custom tier overrides.
- `tests/test_e2e_paper.py`: offline signal-to-trade smoke covering decision-engine approval, paper execution, trade persistence, position persistence, and dashboard visibility on a temporary SQLite DB.
- `tests/test_funding_analysis.py`: focused coverage for bundled-buyer detection thresholds, diverse and unknown funding outcomes, empty input neutrality, provider-failure degradation, and timestamp window filtering without live network calls.
- `tests/test_funding_provider.py`: focused provider coverage for Helius inbound SOL normalization, missing-key and HTTP/provider failure degradation, malformed JSON handling, irrelevant transfer filtering, and explicit confirmation that fake fetchers prevent live network calls.
- `tests/test_onchain_provider.py`: focused provider coverage for `OnChainMonitor` interface conformance, deterministic DexScreener scoring helpers, graceful provider degradation, mint-level dedupe, and explicit confirmation that no wallet/trading code is involved.
- `tests/test_pump_fun_provider.py`: focused provider-shape coverage for live-style pump.fun create payloads, graduation detection from migration fields, and safe handling of websocket ack/malformed messages.
- `tests/test_rugcheck.py`: focused client coverage for successful RugCheck normalization plus graceful handling of missing fields, non-200 responses, timeouts/provider errors, malformed JSON, and non-object payloads.
- `tests/test_twitter_provider.py`: focused provider coverage for Twitter/X payload normalization, no-key degradation, ticker/mint extraction helpers, mention-velocity scoring, and post-ID deduplication without live network calls.
- `tests/test_whale_tracker_provider.py`: focused provider tests for dotenv-based Helius key loading, token-account polling params, Helius-style buy normalization, dedupe, and safe empty/malformed handling.
- `tests/test_strategy.py`: focused coverage for decision-engine risk gating, opt-in liquidity sizing caps/fallbacks, paper sizing diagnostics, and exit-rule behavior.
- `tests/test_monitoring.py`: focused coverage for dashboard DB-path resolution, health compatibility, and one-shot dashboard rendering.

## Last 10 Changes

- 2026-07-07 Wired `src/strategy/dynamic_exits.py` into `src/strategy/exits.py` behind a default-off config gate, adding calibrated 20%-of-peak volume-decay full exits after 15 minutes, 50%-within-60-seconds liquidity emergency exits, testable reason labels, and optional early trail-start signals without changing default exit behavior; focused dynamic-exit pytest and the full suite were rerun in this task.
- 2026-07-07 Wired `RugCheckClient` into the runtime scorer path used by both strict and discovery paper-cycle runs, so valid-looking Solana mints now pre-populate authority, top-holder concentration, and honeypot outcomes before existing threshold checks run while preserving fallback behavior on provider failures or invalid/missing mints; focused RugCheck/risk/CLI pytest and full `python3 -m pytest -q` passed. Latest strict smoke still produced 0 trades but shifted to `top10_holder_check_failed=4` and `liquidity_check_unknown=1` with RugCheck diagnostics `rugcheck_used=5`, `rugcheck_used_top_holder_pct=4`, while the latest discovery smoke produced 0 trades with `top10_holder_check_failed=19`, `liquidity_check_unknown=1`, `holder_lookup_failed_provider=1`, and RugCheck diagnostics `rugcheck_used=20`, `rugcheck_used_top_holder_pct=19`.
- 2026-07-07 Wired the existing liquidity sizing helper into `DecisionEngine` behind a default-off `position.liquidity_sizing_enabled` gate, preserving flat sizing unless enabled while exposing paper-safe sizing diagnostics for liquidity used, cap, reason, and skip/cap outcomes; focused strategy/sizing pytest and the full suite passed.
- 2026-07-07 Wired the pre-live Jito scaffold into `src/execution/jupiter_live.py` through an injected serialized-submission helper that keeps direct live swaps disabled, attempts Jito only when explicitly enabled, falls back to RPC when allowed, and emits structured diagnostics for disabled/attempted/submitted/fallback/no-fallback paths; `python3 -m pytest tests/test_jito.py -q` passed, while full `python3 -m pytest -q` currently still hits unrelated discovery-risk diagnostic failures in `tests/test_cli_paper_cycle.py` and `tests/test_risk.py` outside this task's file scope.
- 2026-07-07 Added `src/risk/funding_provider.py` as a Helius-backed inbound SOL funding adapter for buyer-funder analysis, with env/`.env` API-key resolution, injectable enhanced-transaction fetching, normalization into `InboundTransfer` records, and safe degradation on missing credentials, timeout, non-200, malformed JSON, and irrelevant transfer paths; `python3 -m compileall src/risk/funding_provider.py` and focused funding-provider/funding-analysis pytest passed, while the full `python3 -m pytest -q` suite still hit unrelated discovery-holder diagnostic failures in `tests/test_cli_paper_cycle.py` and `tests/test_risk.py` outside this task's file scope.
- 2026-07-07 Integrated `SignalAggregator`, `OnChainMonitor`, and `TwitterMonitor` into bounded `paper-cycle`, so strict and discovery runs now evaluate ranked multi-source opportunities from pump.fun, whale, on-chain, and Twitter feeds with aggregate source diagnostics; `python3 -m compileall src`, focused CLI/aggregator/on-chain/Twitter pytest, and full `python3 -m pytest -q` all passed. Latest strict smoke collected 5 aggregated opportunities and rejected all 5 on `liquidity_check_unknown`, while the latest discovery smoke evaluated 20 aggregated opportunities, produced 0 paper trades, and hit `top10_holder_check_failed=18`, `liquidity_check_unknown=2` with holder lookup outcomes `holder_lookup_threshold_failed=18`, `holder_lookup_succeeded=1`, and `holder_lookup_failed_provider=1`.
- 2026-07-07 Added `src/risk/funding_analysis.py` as a provider-injected buyer funding analyzer that groups recent inbound SOL funders, reports bundled-buyer concentration metrics, flags common-funder majorities above 50%, and degrades to aggregate unknowns on missing provider data; `python3 -m compileall src/risk/funding_analysis.py`, focused funding-analysis pytest, and the full pytest suite all passed.
- 2026-07-07 Added a pre-live `JitoBlockEngineClient` scaffold that builds bundle-submission payloads from serialized transaction bytes/strings, supports configurable endpoints plus optional validator tip metadata, submits through an injectable HTTP client, and degrades into structured result objects on timeout, non-200, malformed JSON, or provider exceptions; `python3 -m compileall src/chain/jito.py`, focused Jito pytest, and the full pytest suite all passed.
- 2026-07-07 Added `src/strategy/dynamic_exits.py` as a helper-only calibration module for future exit tuning, with deterministic checks for 20%-of-peak volume decay over 15 minutes, configurable trail-start multiples defaulting to `3.0`, 50% liquidity-drop emergencies inside 60 seconds, and aggregate reason-label summaries; `python3 -m compileall src/strategy/dynamic_exits.py`, focused helper pytest, and the full pytest suite all passed.
- 2026-07-07 Added a standalone `RugCheckClient` that fetches `https://api.rugcheck.xyz/v1/tokens/{mint}/report`, normalizes authority flags, top-holder concentration, liquidity state, honeypot indicator, and provider risk score/level into a small result object, and degrades safely across timeout, non-200, malformed JSON, and malformed payload paths; `python3 -m compileall src/risk/rugcheck.py`, focused RugCheck pytest, and the full pytest suite all passed.

## Known Issues

- Live Jupiter execution is still intentionally disabled for direct swaps; the new serialized submission helper only wires optional Jito/RPC routing for future guarded use and does not enable trading by itself.
- Whale tracker is polling-only for now; webhook ingestion still needs a public callback endpoint and receiver.
- pump.fun websocket token-creation and migration subscriptions are now verified against PumpPortal, but the short dry-run did not capture a live migration payload; graduation normalization is still proven by offline fixtures rather than observed runtime traffic.
- Risk checks use conservative local token fields until on-chain enrichment is implemented.
- Dynamic exits are only active when `exits.dynamic_exits_enabled` is enabled and the exit runtime receives `DynamicExitState` context.
- Latest integrated paper-cycle runs still produced 0 paper trades. With RugCheck now wired, the remaining next blockers are predominantly real `top10_holder_check_failed` rejections in both strict and discovery mode, plus a smaller `liquidity_check_unknown` remainder and occasional holder-lookup provider fallback when RugCheck lacks concentration data.
- Liquidity-aware sizing remains disabled by default; operators must opt in through `position.liquidity_sizing_enabled` before the new conservative liquidity caps affect decisions.
