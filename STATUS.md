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
- `src/signals/` defines async signal-source contracts and placeholder source implementations.
- `src/strategy/` contains decision, portfolio, position, and exit helpers.
- `src/monitoring/` contains lightweight health, alert, and dashboard placeholders.

## File Map

- `.env.example`: expected environment variables for RPC, APIs, wallet, and position caps.
- `pyproject.toml`: Python package metadata, dependencies, dev dependencies, pytest, and Ruff config.
- `config/settings.yaml`: risk, position, exit, execution, and monitoring defaults.
- `scripts/check_token.py`: smoke-check script for building a token model and running risk scoring.
- `tests/`: smoke tests for risk contracts, signal aggregation, and paper execution.

## Last 10 Changes

- 2026-07-02 CT-079: Created Phase 1 scaffold and project metadata for Memecoin Trader.

## Known Issues

- Live Jupiter execution is intentionally not implemented yet.
- Signal sources currently return empty lists until provider integrations are added.
- Risk checks use conservative local token fields until on-chain enrichment is implemented.
