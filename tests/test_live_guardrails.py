from typer.testing import CliRunner

import src.cli as cli_module
from src.core.config import load_settings
from src.execution.live_guardrails import evaluate_live_guardrails


runner = CliRunner()


def _live_settings():
    settings = load_settings()
    return settings.model_copy(update={"execution": settings.execution.model_copy(update={"mode": "live"})})


def test_default_config_environment_fails_closed_for_live_trading() -> None:
    decision = evaluate_live_guardrails(load_settings())

    assert decision.allowed is False
    assert "execution_mode_not_live" in decision.diagnostics
    assert "live_trading_env_not_enabled" in decision.diagnostics
    assert "requested_trade_sol_missing" in decision.diagnostics


def test_paper_mode_never_arms_even_with_live_env_present() -> None:
    settings = load_settings()
    decision = evaluate_live_guardrails(
        settings,
        requested_trade_sol=0.01,
        env={
            "LIVE_TRADING_ENABLED": "true",
            "LIVE_CONFIRMATION_PHRASE": settings.live_guardrails.confirmation_phrase,
            "LIVE_KILL_SWITCH": "false",
            "MAX_LIVE_TRADE_SOL": "0.01",
            "MAX_LIVE_DAILY_TRADES": "3",
            "MAX_LIVE_DAILY_LOSS_SOL": "0.05",
        },
    )

    assert decision.allowed is False
    assert decision.diagnostics == ("execution_mode_not_live",)


def test_live_mode_without_explicit_enable_fails_closed() -> None:
    settings = _live_settings()
    decision = evaluate_live_guardrails(
        settings,
        requested_trade_sol=0.01,
        env={
            "LIVE_CONFIRMATION_PHRASE": settings.live_guardrails.confirmation_phrase,
            "LIVE_KILL_SWITCH": "false",
            "MAX_LIVE_TRADE_SOL": "0.01",
            "MAX_LIVE_DAILY_TRADES": "3",
            "MAX_LIVE_DAILY_LOSS_SOL": "0.05",
        },
    )

    assert decision.allowed is False
    assert "live_trading_env_not_enabled" in decision.diagnostics


def test_kill_switch_blocks_live_trading() -> None:
    settings = _live_settings()
    decision = evaluate_live_guardrails(
        settings,
        requested_trade_sol=0.01,
        env={
            "LIVE_TRADING_ENABLED": "true",
            "LIVE_CONFIRMATION_PHRASE": settings.live_guardrails.confirmation_phrase,
            "LIVE_KILL_SWITCH": "true",
            "MAX_LIVE_TRADE_SOL": "0.01",
            "MAX_LIVE_DAILY_TRADES": "3",
            "MAX_LIVE_DAILY_LOSS_SOL": "0.05",
        },
    )

    assert decision.allowed is False
    assert "live_kill_switch_not_explicitly_false" in decision.diagnostics


def test_oversized_trade_is_blocked_by_tiny_live_max() -> None:
    settings = _live_settings()
    decision = evaluate_live_guardrails(
        settings,
        requested_trade_sol=0.02,
        env={
            "LIVE_TRADING_ENABLED": "true",
            "LIVE_CONFIRMATION_PHRASE": settings.live_guardrails.confirmation_phrase,
            "LIVE_KILL_SWITCH": "false",
            "MAX_LIVE_TRADE_SOL": "0.01",
            "MAX_LIVE_DAILY_TRADES": "3",
            "MAX_LIVE_DAILY_LOSS_SOL": "0.05",
        },
    )

    assert decision.allowed is False
    assert "requested_trade_sol_exceeds_max_live_trade_sol" in decision.diagnostics


def test_fully_armed_fixture_can_pass_guardrails_without_network_calls() -> None:
    settings = _live_settings()
    decision = evaluate_live_guardrails(
        settings,
        requested_trade_sol=0.01,
        env={
            "LIVE_TRADING_ENABLED": "true",
            "LIVE_CONFIRMATION_PHRASE": settings.live_guardrails.confirmation_phrase,
            "LIVE_KILL_SWITCH": "false",
            "MAX_LIVE_TRADE_SOL": "0.01",
            "MAX_LIVE_DAILY_TRADES": "3",
            "MAX_LIVE_DAILY_LOSS_SOL": "0.05",
        },
    )

    assert decision.allowed is True
    assert decision.diagnostics == ("live_guardrails_passed",)


def test_malformed_daily_caps_fail_closed() -> None:
    settings = _live_settings()
    decision = evaluate_live_guardrails(
        settings,
        requested_trade_sol=0.01,
        env={
            "LIVE_TRADING_ENABLED": "true",
            "LIVE_CONFIRMATION_PHRASE": settings.live_guardrails.confirmation_phrase,
            "LIVE_KILL_SWITCH": "false",
            "MAX_LIVE_TRADE_SOL": "0.01",
            "MAX_LIVE_DAILY_TRADES": "not-a-number",
            "MAX_LIVE_DAILY_LOSS_SOL": "0.05",
        },
    )

    assert decision.allowed is False
    assert "max_live_daily_trades_invalid" in decision.diagnostics


def test_show_config_surfaces_live_guardrail_diagnostics_without_secret_phrase(monkeypatch) -> None:
    monkeypatch.delenv("LIVE_CONFIRMATION_PHRASE", raising=False)
    monkeypatch.setenv("LIVE_TRADING_ENABLED", "false")
    monkeypatch.setenv("LIVE_KILL_SWITCH", "true")

    result = runner.invoke(cli_module.app, ["show-config"])

    assert result.exit_code == 0
    assert "live_guardrails_diagnostics" in result.stdout
    assert "live_trading_env_not_enabled" in result.stdout
    assert "requested_trade_sol_missing" in result.stdout
    assert "I_UNDERSTAND_THIS_CAN_LOSE_REAL_SOL" not in result.stdout
