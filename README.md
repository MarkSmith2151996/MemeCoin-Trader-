# Memecoin Trader

Automated Solana meme coin trading scaffold for detecting early opportunities, evaluating token risk, and routing trades through paper or live execution adapters.

Default mode is paper trading. Live Jupiter execution is intentionally a placeholder until wallet, RPC, and risk-gate implementation work is completed.

## Layers

1. Signal Detection: social, pump.fun, whale, and on-chain signal sources.
2. Risk Analysis: liquidity, holder distribution, contract authority checks, and aggregate scoring.
3. Decision Engine: risk-gated buy/sell decisions.
4. Execution: paper adapter now, Jupiter live adapter later.
5. Position Management: portfolio caps, partial exits, stops, and health monitoring.

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
cp .env.example .env
```

## Validation

```bash
python -m compileall src
python -m pytest -q
```

## Safety Defaults

- `config/settings.yaml` defaults to `execution.mode: paper`.
- `JupiterLiveExecutionAdapter` raises `NotImplementedError` for swaps until live trading is implemented intentionally.
- `RiskAssessment.all_checks_pass` must be true before a buy decision can pass.

## CLI

```bash
python -m src.cli health
python scripts/check_token.py <mint_address>
```
