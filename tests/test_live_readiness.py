import asyncio
from datetime import UTC, datetime
from pathlib import Path

from typer.testing import CliRunner

import src.cli as cli_module
from src.core.config import load_settings
from src.core.database import init_db
from src.core.models import CheckResult, RiskAssessment, Signal, SignalSource, SignalType, TokenInfo
from src.execution.base import ExecutionAdapter
from src.execution.live_circuit_breaker import LiveCircuitBreaker
from src.execution.live_preflight import TransactionSimulationResult
from src.execution.live_readiness import evaluate_micro_live_readiness
from src.monitoring.health import HealthStatus
from src.strategy.decision_engine import DecisionEngine
from src.strategy.position_manager import PositionManager


runner = CliRunner()


class SmokePaperExecutionAdapter(ExecutionAdapter):
    def __init__(self, price_sol: float = 0.00001) -> None:
        self.price_sol = price_sol

    async def execute_swap(self, mint_address, side, amount_sol, slippage_bps=300):
        from src.core.models import Trade

        return Trade(
            mint_address=mint_address,
            side=side,
            amount_sol=amount_sol,
            token_amount=amount_sol / self.price_sol,
            price_sol=self.price_sol,
            slippage_bps=slippage_bps,
            tx_signature="READINESS-PAPER-1",
            mode=self.mode,
            status="simulated",
        )

    async def get_quote(self, mint_address, side, amount_sol, slippage_bps=300):
        raise NotImplementedError

    async def get_current_price(self, mint_address):
        return self.price_sol

    async def close(self):
        return None

    @property
    def mode(self):
        return "paper"


class PassingRiskScorer:
    def __init__(self, assessment: RiskAssessment) -> None:
        self.assessment = assessment

    async def assess_signal(self, signal: Signal) -> RiskAssessment:
        return self.assessment


def _live_settings():
    settings = load_settings()
    return settings.model_copy(
        update={
            "execution": settings.execution.model_copy(
                update={"mode": "live", "primary_rpc_url": "https://primary.example"}
            )
        }
    )


def _armed_env(settings):
    return {
        "LIVE_TRADING_ENABLED": "true",
        "LIVE_CONFIRMATION_PHRASE": settings.live_guardrails.confirmation_phrase,
        "LIVE_KILL_SWITCH": "false",
        "PRIMARY_RPC_URL": "https://primary.example",
        "MAX_LIVE_TRADE_SOL": "0.01",
        "MAX_LIVE_DAILY_TRADES": "3",
        "MAX_LIVE_DAILY_LOSS_SOL": "0.05",
        "MIN_LIVE_WALLET_BALANCE_SOL": "0.05",
    }


def _assessment(mint_address: str) -> RiskAssessment:
    return RiskAssessment(
        token=TokenInfo(
            mint_address=mint_address,
            liquidity_sol=100.0,
            unique_buyers=250,
            top10_holder_pct=12.0,
            creator_holding_pct=2.5,
            mint_authority_revoked=True,
            freeze_authority_revoked=True,
        ),
        liquidity_check=CheckResult.PASS,
        top10_holder_check=CheckResult.PASS,
        creator_holding_check=CheckResult.PASS,
        age_check=CheckResult.PASS,
        unique_buyers_check=CheckResult.PASS,
        mint_authority_check=CheckResult.PASS,
        freeze_authority_check=CheckResult.PASS,
        honeypot_check=CheckResult.PASS,
        score=0.0,
        reasons=[],
    )


async def _seed_position_manager(db_path: Path):
    settings = load_settings()
    await init_db(db_path)
    manager = PositionManager(db_path, settings)
    engine = DecisionEngine(
        SmokePaperExecutionAdapter(),
        PassingRiskScorer(_assessment("readiness-mint")),
        manager,
        settings,
        db=db_path,
    )
    signal = Signal(source=SignalSource.PUMP_FUN, type=SignalType.NEW_POOL, mint_address="readiness-mint", confidence=0.8)
    trade = await engine.evaluate_signal(signal)
    assert trade is not None
    return manager


def test_fully_missing_live_readiness_reports_not_ready() -> None:
    async def run() -> None:
        report = await evaluate_micro_live_readiness(load_settings())

        assert report.ready is False
        checks = {check.name: check for check in report.checks}
        assert checks["guardrails"].ok is False
        assert checks["preflight"].ok is False
        assert checks["position_reconciliation"].ok is False
        assert checks["circuit_breaker"].ok is False

    asyncio.run(run())


def test_stale_health_and_tripped_breaker_surface_clearly() -> None:
    async def run() -> None:
        settings = _live_settings()
        breaker = LiveCircuitBreaker(rpc_failure_threshold=1)
        breaker.record_health_check(True, observed_at=datetime.now(UTC))
        breaker.record_rpc_failure()
        report = await evaluate_micro_live_readiness(
            settings,
            env=_armed_env(settings),
            requested_trade_sol=0.01,
            wallet_balance_lookup=lambda: _async_return(1.0),
            transaction_simulator=lambda _tx: _async_return(TransactionSimulationResult(ok=True)),
            circuit_breaker=breaker,
            health_status=HealthStatus(ok=False, message="bad", checked_at=datetime.now(UTC)),
        )

        assert report.ready is False
        checks = {check.name: check for check in report.checks}
        assert checks["circuit_breaker"].ok is False
        assert "rpc_failure_threshold_reached" in checks["circuit_breaker"].diagnostics
        assert checks["health"].ok is False
        assert checks["health"].diagnostics == ("health_check_failed",)

    asyncio.run(run())


def test_fully_armed_fake_ready_fixture_reports_ready(tmp_path: Path) -> None:
    async def run() -> None:
        settings = _live_settings()
        manager = await _seed_position_manager(tmp_path / "ready.db")
        breaker = LiveCircuitBreaker()
        breaker.record_health_check(True, observed_at=datetime.now(UTC))

        async def wallet_holdings_lookup():
            position = await manager.get_position("readiness-mint")
            assert position is not None
            return {"readiness-mint": position.token_amount}

        report = await evaluate_micro_live_readiness(
            settings,
            env=_armed_env(settings),
            requested_trade_sol=0.01,
            wallet_balance_lookup=lambda: _async_return(1.0),
            transaction_simulator=lambda _tx: _async_return(TransactionSimulationResult(ok=True)),
            position_manager=manager,
            wallet_holdings_lookup=wallet_holdings_lookup,
            circuit_breaker=breaker,
            health_status=HealthStatus(ok=True, message="ok", checked_at=datetime.now(UTC)),
        )

        assert report.ready is True
        assert all(check.ok for check in report.checks)

    asyncio.run(run())


def test_live_readiness_cli_reports_not_ready_by_default(tmp_path: Path) -> None:
    db = tmp_path / "readiness.db"
    result = runner.invoke(cli_module.app, ["live-readiness", "--db-path", str(db)])

    assert result.exit_code == 0
    assert "micro_live_ready=NOT READY" in result.stdout
    assert "guardrails=not_ready" in result.stdout
    assert "preflight=not_ready" in result.stdout
    assert "position_reconciliation=not_ready diagnostics=wallet_holdings_lookup_unavailable" in result.stdout
    assert "circuit_breaker=ok diagnostics=paper_mode_unaffected" in result.stdout
    assert "I_UNDERSTAND_THIS_CAN_LOSE_REAL_SOL" not in result.stdout


def test_preflight_and_recon_unavailable_include_env_hints() -> None:
    async def run() -> None:
        report = await evaluate_micro_live_readiness(load_settings())
        checks = {check.name: check for check in report.checks}

        preflight = checks["preflight"]
        assert not preflight.ok
        assert "wallet_balance_lookup_unavailable" in preflight.diagnostics
        assert "HELIUS_API_KEY" in preflight.recommended_env
        assert "TRADING_WALLET_PRIVATE_KEY" in preflight.recommended_env

        recon = checks["position_reconciliation"]
        assert not recon.ok
        assert recon.diagnostics == ("position_reconciliation_unavailable",)
        assert recon.recommended_env == ()

    asyncio.run(run())


def test_configured_but_unhealthy_providers_surface_clearly() -> None:
    async def thrower(_tx=None):
        msg = "simulation rpc error"
        raise RuntimeError(msg)

    async def none_balance() -> float | None:
        return None

    async def none_holdings() -> dict[str, float] | None:
        return None

    async def run() -> None:
        settings = _live_settings()
        breaker = LiveCircuitBreaker(rpc_failure_threshold=1)
        breaker.record_health_check(True, observed_at=datetime.now(UTC))
        breaker.record_rpc_failure()

        report = await evaluate_micro_live_readiness(
            settings,
            env=_armed_env(settings),
            requested_trade_sol=0.01,
            wallet_balance_lookup=none_balance,
            transaction_simulator=thrower,
            position_manager=None,
            wallet_holdings_lookup=none_holdings,
            circuit_breaker=breaker,
            health_status=HealthStatus(ok=False, message="bad", checked_at=datetime.now(UTC)),
        )

        assert not report.ready
        checks = {check.name: check for check in report.checks}

        assert not checks["preflight"].ok
        assert "wallet_balance_unknown" in checks["preflight"].diagnostics
        assert "transaction_simulation_failed_provider" in checks["preflight"].diagnostics

        assert not checks["position_reconciliation"].ok
        assert "position_reconciliation_unavailable" in checks["position_reconciliation"].diagnostics

        assert not checks["circuit_breaker"].ok
        assert "rpc_failure_threshold_reached" in checks["circuit_breaker"].diagnostics

        assert not checks["health"].ok
        assert "health_check_failed" in checks["health"].diagnostics

    asyncio.run(run())


def test_position_recon_diagnostics_split_by_missing_provider(tmp_path: Path) -> None:
    async def run() -> None:
        settings = _live_settings()

        # Neither position_manager nor wallet_holdings_lookup -> position_reconciliation_unavailable
        report = await evaluate_micro_live_readiness(settings, position_manager=None, wallet_holdings_lookup=None)
        recon = {c.name: c for c in report.checks}["position_reconciliation"]
        assert recon.diagnostics == ("position_reconciliation_unavailable",)
        assert recon.recommended_env == ()

        # position_manager provided but no wallet_holdings_lookup -> wallet_holdings_lookup_unavailable
        manager = await _seed_position_manager(tmp_path / "split.db")
        report2 = await evaluate_micro_live_readiness(settings, position_manager=manager, wallet_holdings_lookup=None)
        recon2 = {c.name: c for c in report2.checks}["position_reconciliation"]
        assert recon2.diagnostics == ("wallet_holdings_lookup_unavailable",)
        assert "HELIUS_API_KEY" in recon2.recommended_env

    asyncio.run(run())


async def _async_return(value):
    return value
