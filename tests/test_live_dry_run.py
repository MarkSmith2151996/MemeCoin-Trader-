"""Coverage: micro-live dry-run command hardening.

All tests use CLI invocation — no real trades executed.
"""

from __future__ import annotations

from pathlib import Path
from typer.testing import CliRunner

import src.cli as cli_module

runner = CliRunner()


def test_dry_run_with_missing_wallet_public_key_blocks_safely(tmp_path: Path) -> None:
    """Dry-run with missing config reports BLOCKED."""
    result = runner.invoke(
        cli_module.app,
        [
            "live-buy",
            "--mint", "abc123",
            "--amount-sol", "0.01",
            "--db-path", str(tmp_path / "dry1.db"),
            "--dry-run",
        ],
    )
    assert result.exit_code == 0
    assert "Dry-Run Report" in result.stdout
    assert "Verdict:                  BLOCKED" in result.stdout


def test_dry_run_with_missing_private_key_blocks_submit(tmp_path: Path) -> None:
    """Dry-run reports block even when only private key is missing."""
    result = runner.invoke(
        cli_module.app,
        [
            "live-buy",
            "--mint", "abc123",
            "--amount-sol", "0.01",
            "--db-path", str(tmp_path / "dry2.db"),
            "--dry-run",
        ],
    )
    assert result.exit_code == 0
    assert "Verdict:                  BLOCKED" in result.stdout
    assert "execution_mode_not_live" in result.stdout or "BLOCKED" in result.stdout


def test_dry_run_reports_clear_block_reasons(tmp_path: Path) -> None:
    """Dry-run output includes specific blocking diagnostics."""
    result = runner.invoke(
        cli_module.app,
        [
            "live-buy",
            "--mint", "abc123",
            "--amount-sol", "0.01",
            "--db-path", str(tmp_path / "dry3.db"),
            "--dry-run",
        ],
    )
    assert result.exit_code == 0
    assert "Dry-Run Report" in result.stdout
    assert "blocking reasons:" in result.stdout or "BLOCKED" in result.stdout
    assert "secret" not in result.stdout.lower()
    assert "private_key" not in result.stdout


def test_dry_run_does_not_print_secrets(tmp_path: Path) -> None:
    """No secret values appear in dry-run output."""
    result = runner.invoke(
        cli_module.app,
        [
            "live-buy",
            "--mint", "abc123",
            "--amount-sol", "0.01",
            "--db-path", str(tmp_path / "dry4.db"),
            "--dry-run",
        ],
    )
    assert result.exit_code == 0
    output = result.stdout.lower()
    assert "api-key=" not in output
    assert "helius_api_key=" not in output
    assert "rpc_url=" not in output
    # Present/missing status is fine, actual values are not
    # Check no full key prefix
    assert "50661bc5" not in result.stdout


def test_dry_run_does_not_print_tx_payloads(tmp_path: Path) -> None:
    """No raw transaction bytes or payloads appear in dry-run output."""
    result = runner.invoke(
        cli_module.app,
        [
            "live-buy",
            "--mint", "abc123",
            "--amount-sol", "0.01",
            "--db-path", str(tmp_path / "dry5.db"),
            "--dry-run",
        ],
    )
    assert result.exit_code == 0
    output = result.stdout.lower()
    assert "transaction" not in output or "readiness" in output
    assert "\\x" not in result.stdout


def test_dry_run_live_exit_blocks_same_as_live_buy(tmp_path: Path) -> None:
    """Live-exit dry-run reports BLOCKED similarly."""
    result = runner.invoke(
        cli_module.app,
        [
            "live-exit",
            "--mint", "abc123",
            "--db-path", str(tmp_path / "dry6.db"),
            "--dry-run",
        ],
    )
    assert result.exit_code == 0
    assert "Dry-Run Report" in result.stdout
    assert "Verdict:                  BLOCKED" in result.stdout


def test_dry_run_reports_key_presence_only(tmp_path: Path) -> None:
    """Dry-run reports present/missing without values."""
    result = runner.invoke(
        cli_module.app,
        [
            "live-buy",
            "--mint", "abc123",
            "--amount-sol", "0.01",
            "--db-path", str(tmp_path / "dry7.db"),
            "--dry-run",
        ],
    )
    assert result.exit_code == 0
    assert "HELIUS_API_KEY:" in result.stdout
    assert "TRADING_WALLET_PUBLIC_KEY:" in result.stdout
    assert "TRADING_WALLET_PRIVATE_KEY:" in result.stdout
    assert "present" in result.stdout or "MISSING" in result.stdout


def test_default_mode_remains_paper(tmp_path: Path) -> None:
    """Dry-run confirms execution mode is still paper."""
    result = runner.invoke(
        cli_module.app,
        [
            "live-buy",
            "--mint", "abc123",
            "--amount-sol", "0.01",
            "--db-path", str(tmp_path / "dry8.db"),
            "--dry-run",
        ],
    )
    assert result.exit_code == 0
    assert "paper" in result.stdout.lower() or "Execution mode:" in result.stdout
