"""Regression tests: no secrets leak through any CLI output.

Uses fake secret strings locally; never reads real env values.
"""

from __future__ import annotations

import os
from pathlib import Path
from typer.testing import CliRunner

import src.cli as cli_module
from src.execution.env_readiness import evaluate_env_readiness

runner = CliRunner()

FAKE_HELIUS_KEY = "50661bc5-1111-2222-3333-444444444444"
FAKE_PUB_KEY = "FAKE_PUBLIC_KEY_1234567890"
FAKE_PRIV_KEY = "FAKE_PRIVATE_KEY_1234567890abcdef"
FAKE_HELIUS_PREFIX = "50661bc5"


def test_env_readiness_no_full_api_key() -> None:
    """env-readiness never prints the full HELIUS_API_KEY value."""
    result = runner.invoke(cli_module.app, ["env-readiness"])
    assert result.exit_code == 0
    assert FAKE_HELIUS_PREFIX not in result.stdout
    assert "api-key=" not in result.stdout.lower()


def test_env_readiness_no_partial_api_key() -> None:
    """No API key prefix or fragment appears."""
    result = runner.invoke(cli_module.app, ["env-readiness"])
    assert result.exit_code == 0
    output = result.stdout
    assert f"{FAKE_HELIUS_PREFIX}" not in output


def test_live_readiness_no_api_key() -> None:
    """live-readiness never prints the API key."""
    result = runner.invoke(cli_module.app, ["live-readiness", "--db-path", "/tmp/nonexistent_redact.db"])
    assert result.exit_code == 0
    assert FAKE_HELIUS_PREFIX not in result.stdout
    assert "api-key=" not in result.stdout.lower()


def test_live_readiness_no_private_key() -> None:
    """live-readiness never prints the private key."""
    result = runner.invoke(cli_module.app, ["live-readiness", "--db-path", "/tmp/nonexistent_redact.db"])
    assert result.exit_code == 0
    output = result.stdout.lower()
    assert "private_key" not in output


def test_dry_run_no_api_key() -> None:
    """live-buy --dry-run never prints the API key."""
    result = runner.invoke(
        cli_module.app,
        ["live-buy", "--mint", "abc", "--amount-sol", "0.01", "--dry-run", "--db-path", "/tmp/nonexistent_redact.db"],
    )
    assert result.exit_code == 0
    assert FAKE_HELIUS_PREFIX not in result.stdout
    assert "api-key=" not in result.stdout.lower()


def test_dry_run_no_private_key() -> None:
    """live-buy --dry-run never prints the private key."""
    result = runner.invoke(
        cli_module.app,
        ["live-buy", "--mint", "abc", "--amount-sol", "0.01", "--dry-run", "--db-path", "/tmp/nonexistent_redact.db"],
    )
    assert result.exit_code == 0
    assert FAKE_PRIV_KEY not in result.stdout
    assert "private_key" not in result.stdout or "TRADING_WALLET_PRIVATE_KEY" in result.stdout


def test_dry_run_no_secret_values_at_all() -> None:
    """No known secret value prefixes appear in dry-run output."""
    result = runner.invoke(
        cli_module.app,
        ["live-buy", "--mint", "abc", "--amount-sol", "0.01", "--dry-run", "--db-path", "/tmp/nonexistent_redact.db"],
    )
    assert result.exit_code == 0
    output = result.stdout
    assert FAKE_HELIUS_PREFIX not in output
    assert FAKE_PUB_KEY not in output
    assert FAKE_PRIV_KEY not in output
    assert "api-key=" not in output.lower()


def test_env_readiness_with_fake_env_no_leak(monkeypatch) -> None:
    """Even when env vars are set, env-readiness does not leak values."""
    env = {
        "HELIUS_API_KEY": FAKE_HELIUS_KEY,
        "TRADING_WALLET_PUBLIC_KEY": FAKE_PUB_KEY,
        "TRADING_WALLET_PRIVATE_KEY": FAKE_PRIV_KEY,
        "LIVE_TRADING_ENABLED": "true",
        "LIVE_CONFIRMATION_PHRASE": "I_UNDERSTAND",
        "LIVE_KILL_SWITCH": "false",
        "MAX_LIVE_TRADE_SOL": "0.005",
        "MAX_DAILY_LIVE_TRADES": "1",
        "MAX_DAILY_LOSS_SOL": "0.02",
        "PRIMARY_RPC_URL": "https://primary.example.com/rpc",
        "BACKUP_RPC_URL": "https://backup.example.com/rpc",
    }
    for k, v in env.items():
        monkeypatch.setenv(k, v)

    result = runner.invoke(cli_module.app, ["env-readiness"])
    assert result.exit_code == 0
    output = result.stdout

    assert FAKE_HELIUS_KEY not in output
    assert FAKE_HELIUS_PREFIX not in output
    assert FAKE_PUB_KEY not in output
    assert FAKE_PRIV_KEY not in output
    assert "primary.example.com" not in output
    assert "backup.example.com" not in output
    assert "I_UNDERSTAND" not in output
    assert "50661bc5" not in output


def test_paper_soak_no_secrets(tmp_path: Path) -> None:
    """Paper-soak audit output does not contain secrets."""
    result = runner.invoke(
        cli_module.app,
        ["paper-soak", "--max-signals", "1", "--timeout-seconds", "0.1", "--db-path", str(tmp_path / "redact-soak.db")],
    )
    assert result.exit_code == 0
    output = result.stdout
    assert FAKE_HELIUS_PREFIX not in output
    assert "api-key=" not in output.lower()


def test_live_exit_dry_run_no_secrets(tmp_path: Path) -> None:
    """live-exit --dry-run never prints secrets."""
    result = runner.invoke(
        cli_module.app,
        ["live-exit", "--mint", "abc", "--dry-run", "--db-path", str(tmp_path / "redact-exit.db")],
    )
    assert result.exit_code == 0
    assert FAKE_HELIUS_PREFIX not in result.stdout
    assert "api-key=" not in result.stdout.lower()


def test_show_config_no_confirmation_phrase(monkeypatch) -> None:
    """show-config redacts confirmation phrase."""
    monkeypatch.setenv("LIVE_CONFIRMATION_PHRASE", "I_UNDERSTAND_THE_RISKS")
    result = runner.invoke(cli_module.app, ["show-config"])
    assert result.exit_code == 0
    assert "I_UNDERSTAND_THE_RISKS" not in result.stdout
    assert "<redacted>" in result.stdout or "confirmation_phrase" not in result.stdout
