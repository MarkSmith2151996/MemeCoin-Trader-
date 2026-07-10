from typer.testing import CliRunner

import src.cli as cli_module
from src.core.config import load_settings
from src.execution.live_execution_config import evaluate_live_execution_config


runner = CliRunner()


def _live_settings():
    settings = load_settings()
    return settings.model_copy(update={"execution": settings.execution.model_copy(update={"mode": "live"})})


def test_default_config_is_safe_in_paper_mode() -> None:
    decision = evaluate_live_execution_config(load_settings())

    assert decision.allowed is True
    assert decision.diagnostics == ("live_execution_config_valid",)


def test_malformed_priority_fee_config_fails_closed() -> None:
    settings = _live_settings()
    decision = evaluate_live_execution_config(settings, env={"PRIORITY_FEE_LAMPORTS": "not-a-number"})

    assert decision.allowed is False
    assert "priority_fee_config_invalid" in decision.diagnostics


def test_over_limit_priority_fee_is_blocked() -> None:
    settings = _live_settings().model_copy(
        update={
            "execution": _live_settings().execution.model_copy(
                update={"priority_fee_lamports": 200_000}
            )
        }
    )
    decision = evaluate_live_execution_config(settings, env={"PRIMARY_RPC_URL": "https://rpc.example"})

    assert decision.allowed is False
    assert "priority_fee_out_of_bounds" in decision.diagnostics


def test_show_config_redacts_rpc_urls(monkeypatch) -> None:
    monkeypatch.setenv("PRIMARY_RPC_URL", "https://primary.example/?api-key=secret")
    monkeypatch.setenv("BACKUP_RPC_URL", "https://backup.example/?token=secret")

    result = runner.invoke(cli_module.app, ["show-config"])

    assert result.exit_code == 0
    assert "live_execution_config_diagnostics" in result.stdout
    assert "primary.example" in result.stdout
    assert "backup.example" in result.stdout
    assert "api-key=secret" not in result.stdout
    assert "token=secret" not in result.stdout


def test_rpc_labels_never_include_basic_auth() -> None:
    decision = evaluate_live_execution_config(
        _live_settings(),
        env={"PRIMARY_RPC_URL": "https://user:password@primary.example/rpc?api-key=secret"},
    )

    assert decision.primary_rpc_label == "primary.example"
    assert "user" not in str(decision)
    assert "password" not in str(decision)
