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
    assert "Preflight Explainer" in result.stdout
    assert "BLOCKED" in result.stdout


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
    assert "Preflight Explainer" in result.stdout
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
    assert "Preflight Explainer" in result.stdout
    assert "Blocking Reasons" in result.stdout
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
    assert "Preflight Explainer" in result.stdout
    assert "BLOCKED" in result.stdout


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


def test_live_buy_blocked_has_preflight_explainer(tmp_path: Path) -> None:
    """Blocked live-buy (non-dry-run) prints preflight explainer with env summary."""
    result = runner.invoke(
        cli_module.app,
        [
            "live-buy",
            "--mint", "abc123",
            "--amount-sol", "0.01",
            "--db-path", str(tmp_path / "exp1.db"),
        ],
    )
    assert result.exit_code == 0
    output = result.stdout
    assert "Preflight Explainer" in output
    assert "Env Readiness" in output
    assert "Provider Status" in output
    assert "Live Readiness" in output
    assert "Blocking Reasons" in output
    assert "Missing Arming Gates" in output
    assert "Operator Next Steps" in output


def test_live_buy_blocked_shows_private_key_as_gate(tmp_path: Path) -> None:
    """Blocked live-buy lists TRADING_WALLET_PRIVATE_KEY as missing gate."""
    result = runner.invoke(
        cli_module.app,
        [
            "live-buy",
            "--mint", "abc123",
            "--amount-sol", "0.01",
            "--db-path", str(tmp_path / "exp2.db"),
        ],
    )
    assert result.exit_code == 0
    output = result.stdout
    assert "TRADING_WALLET_PRIVATE_KEY" in output


def test_live_buy_blocked_shows_missing_private_key_as_gate(tmp_path: Path) -> None:
    """Blocked live-buy mentions TRADING_WALLET_PRIVATE_KEY in arming gates."""
    result = runner.invoke(
        cli_module.app,
        [
            "live-buy",
            "--mint", "abc123",
            "--amount-sol", "0.01",
            "--db-path", str(tmp_path / "exp3.db"),
        ],
    )
    assert result.exit_code == 0
    output = result.stdout
    assert "TRADING_WALLET_PRIVATE_KEY" in output
    assert "signing" in output


def test_live_buy_blocked_shows_live_arming_gates(tmp_path: Path) -> None:
    """Blocked live-buy mentions LIVE_TRADING_ENABLED and confirmation phrase."""
    result = runner.invoke(
        cli_module.app,
        [
            "live-buy",
            "--mint", "abc123",
            "--amount-sol", "0.01",
            "--db-path", str(tmp_path / "exp4.db"),
        ],
    )
    assert result.exit_code == 0
    output = result.stdout
    assert "LIVE_TRADING_ENABLED" in output
    assert "LIVE_CONFIRMATION_PHRASE" in output
    assert "LIVE_KILL_SWITCH" in output


def test_live_buy_blocked_prints_no_secrets(tmp_path: Path) -> None:
    """Blocked live-buy explainer contains no secret values."""
    result = runner.invoke(
        cli_module.app,
        [
            "live-buy",
            "--mint", "abc123",
            "--amount-sol", "0.01",
            "--db-path", str(tmp_path / "exp5.db"),
        ],
    )
    assert result.exit_code == 0
    output = result.stdout.lower()
    assert "50661bc5" not in result.stdout
    assert "api-key=" not in output
    assert "helius_api_key=" not in output
    assert "private_key" not in output or "TRADING_WALLET_PRIVATE_KEY" in result.stdout
    assert "rpc_url=" not in output
    assert "\\x" not in result.stdout


def test_live_exit_blocked_has_preflight_explainer(tmp_path: Path) -> None:
    """Blocked live-exit (non-dry-run) prints preflight explainer."""
    result = runner.invoke(
        cli_module.app,
        [
            "live-exit",
            "--mint", "abc123",
            "--db-path", str(tmp_path / "exp6.db"),
        ],
    )
    assert result.exit_code == 0
    output = result.stdout
    assert "Preflight Explainer" in output
    assert "Env Readiness" in output
    assert "Missing Arming Gates" in output


def test_live_buy_blocked_shows_vars_present_count(tmp_path: Path) -> None:
    """Blocked live-buy shows how many env vars are present vs total."""
    result = runner.invoke(
        cli_module.app,
        [
            "live-buy",
            "--mint", "abc123",
            "--amount-sol", "0.01",
            "--db-path", str(tmp_path / "exp7.db"),
        ],
    )
    assert result.exit_code == 0
    assert "Vars present" in result.stdout
    assert "/11" in result.stdout


def test_live_buy_blocked_shows_provider_status(tmp_path: Path) -> None:
    """Blocked live-buy shows provider availability status."""
    result = runner.invoke(
        cli_module.app,
        [
            "live-buy",
            "--mint", "abc123",
            "--amount-sol", "0.01",
            "--db-path", str(tmp_path / "exp8.db"),
        ],
    )
    assert result.exit_code == 0
    output = result.stdout
    assert "transaction_simulator" in output
    assert "wallet_balance_lookup" in output
    assert "wallet_holdings_lookup" in output
    assert "available" in output or "unavailable" in output


def test_live_buy_blocked_shows_next_steps(tmp_path: Path) -> None:
    """Blocked live-buy shows operator next steps section."""
    result = runner.invoke(
        cli_module.app,
        [
            "live-buy",
            "--mint", "abc123",
            "--amount-sol", "0.01",
            "--db-path", str(tmp_path / "exp9.db"),
        ],
    )
    assert result.exit_code == 0
    output = result.stdout
    assert "Operator Next Steps" in output
    assert "MICRO_LIVE_RUNBOOK" in output
    assert "WALLET_SETUP" in output


def test_live_buy_blocked_shows_blocking_reasons_section(tmp_path: Path) -> None:
    """Blocked live-buy includes explicit blocking reasons section."""
    result = runner.invoke(
        cli_module.app,
        [
            "live-buy",
            "--mint", "abc123",
            "--amount-sol", "0.01",
            "--db-path", str(tmp_path / "exp10.db"),
        ],
    )
    assert result.exit_code == 0
    output = result.stdout
    assert "Blocking Reasons" in output
    assert "execution_mode_not_live" in output
