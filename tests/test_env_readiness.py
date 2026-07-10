"""Coverage: env-readiness diagnostics are safe and correct."""

from typer.testing import CliRunner

import src.cli as cli_module
from src.execution.env_readiness import ENV_NAMES, evaluate_env_readiness

runner = CliRunner()


def test_all_env_vars_report_missing_by_default() -> None:
    report = evaluate_env_readiness(env={})

    assert not report.all_present()
    items_by_name = {item.name: item for item in report.items}
    assert len(items_by_name) == 11

    for name in ENV_NAMES:
        assert items_by_name[name].present is False


def test_env_vars_report_present_when_set() -> None:
    env = {
        "HELIUS_API_KEY": "hk_abc123",
        "TRADING_WALLET_PUBLIC_KEY": "pubkey123",
        "TRADING_WALLET_PRIVATE_KEY": "privkey123",
        "LIVE_TRADING_ENABLED": "true",
        "LIVE_CONFIRMATION_PHRASE": "I_UNDERSTAND",
        "LIVE_KILL_SWITCH": "false",
        "MAX_LIVE_TRADE_SOL": "0.005",
        "MAX_LIVE_DAILY_TRADES": "1",
        "MAX_LIVE_DAILY_LOSS_SOL": "0.02",
        "PRIMARY_RPC_URL": "https://primary.example",
        "BACKUP_RPC_URL": "https://backup.example",
    }
    report = evaluate_env_readiness(env=env)

    assert report.all_present()
    for item in report.items:
        assert item.present


def test_env_readiness_cli_no_secrets_leaked() -> None:
    result = runner.invoke(cli_module.app, ["env-readiness"])

    assert result.exit_code == 0
    output = result.stdout

    assert "HELIUS_API_KEY=present" in output or "HELIUS_API_KEY=MISSING" in output
    assert "TRADING_WALLET_PUBLIC_KEY=present" in output or "TRADING_WALLET_PUBLIC_KEY=MISSING" in output
    assert "TRADING_WALLET_PRIVATE_KEY=present" in output or "TRADING_WALLET_PRIVATE_KEY=MISSING" in output
    assert "LIVE_TRADING_ENABLED=present" in output or "LIVE_TRADING_ENABLED=MISSING" in output

    assert "hk_" not in output
    assert "privkey" not in output
    assert "pubkey" not in output
    assert "50661bc5" not in output
    assert "I_UNDERSTAND" not in output


def test_env_readiness_cli_lines_format() -> None:
    result = runner.invoke(cli_module.app, ["env-readiness"])

    assert result.exit_code == 0
    lines = [line.strip() for line in result.stdout.split("\n") if line.strip()]

    assert lines[0].startswith("env_readiness_ready=")
    for name in ENV_NAMES:
        assert any(name in line for line in lines)


def test_empty_value_counts_as_missing() -> None:
    env = {"HELIUS_API_KEY": "", "LIVE_TRADING_ENABLED": "  "}
    report = evaluate_env_readiness(env=env)
    items_by_name = {item.name: item for item in report.items}

    assert items_by_name["HELIUS_API_KEY"].present is False
    assert items_by_name["LIVE_TRADING_ENABLED"].present is False
