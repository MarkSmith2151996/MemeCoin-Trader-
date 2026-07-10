import asyncio
from pathlib import Path

from typer.testing import CliRunner

import src.cli as cli_module
from src.core.config import load_settings
from src.core.database import init_db
from src.core.models import CheckResult, RiskAssessment, Signal, SignalSource, SignalType, TokenInfo
from src.execution.base import ExecutionAdapter
from src.execution.live_circuit_breaker import LiveCircuitBreaker
from src.execution.live_exit import execute_guarded_live_exit
from src.execution.live_preflight import TransactionSimulationResult
from src.execution.jupiter_live import JupiterLiveExecutionAdapter
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
            tx_signature="LIVE-EXIT-SEED",
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


async def _seed_position_manager(db_path: Path, mint_address: str = "exit-mint", *, max_single_position_sol: float = 0.5):
    settings = load_settings().model_copy(
        update={
            "position": load_settings().position.model_copy(
                update={"max_single_position_sol": max_single_position_sol, "max_portfolio_sol": max_single_position_sol}
            )
        }
    )
    await init_db(db_path)
    manager = PositionManager(db_path, settings)
    engine = DecisionEngine(
        SmokePaperExecutionAdapter(),
        PassingRiskScorer(_assessment(mint_address)),
        manager,
        settings,
        db=db_path,
    )
    signal = Signal(source=SignalSource.PUMP_FUN, type=SignalType.NEW_POOL, mint_address=mint_address, confidence=0.8)
    trade = await engine.evaluate_signal(signal)
    assert trade is not None
    return manager


class RecordingRpcSubmitter:
    def __init__(self, result: str = "rpc-signature-exit") -> None:
        self.result = result
        self.calls: list[str | bytes] = []

    async def __call__(self, transaction: str | bytes) -> str:
        self.calls.append(transaction)
        return self.result


async def _wallet_holdings_for(manager: PositionManager, mint: str):
    position = await manager.get_position(mint)
    assert position is not None
    return {mint: position.token_amount}


def test_live_exit_cannot_open_new_position(tmp_path: Path) -> None:
    async def run() -> None:
        settings = _live_settings()
        await init_db(tmp_path / "no-position.db")
        manager = PositionManager(tmp_path / "no-position.db", load_settings())
        adapter = JupiterLiveExecutionAdapter(
            rpc_submitter=RecordingRpcSubmitter(),
            settings=settings,
            guardrail_env=_armed_env(settings),
            wallet_balance_lookup=lambda: _async_return(1.0),
            transaction_simulator=lambda _tx: _async_return(TransactionSimulationResult(ok=True)),
            circuit_breaker=LiveCircuitBreaker(),
        )
        breaker = LiveCircuitBreaker()
        breaker.record_health_check(True)

        result = await execute_guarded_live_exit(
            settings=settings,
            mint_address="missing-mint",
            position_manager=manager,
            adapter=adapter,
            exit_transaction_builder=lambda _mint, _amount: _async_return("tx"),
            wallet_holdings_lookup=lambda: _async_return({}),
            wallet_balance_lookup=lambda: _async_return(1.0),
            transaction_simulator=lambda _tx: _async_return(TransactionSimulationResult(ok=True)),
            circuit_breaker=breaker,
            env=_armed_env(settings),
        )

        assert result.ok is False
        assert result.diagnostics == ("position_not_found",)

    asyncio.run(run())


def test_live_exit_failures_in_readiness_block_exit(tmp_path: Path) -> None:
    async def run() -> None:
        settings = _live_settings()
        manager = await _seed_position_manager(tmp_path / "blocked.db")
        breaker = LiveCircuitBreaker(rpc_failure_threshold=1)
        breaker.record_health_check(True)
        breaker.record_rpc_failure()
        adapter = JupiterLiveExecutionAdapter(
            rpc_submitter=RecordingRpcSubmitter(),
            settings=settings,
            guardrail_env=_armed_env(settings),
            wallet_balance_lookup=lambda: _async_return(1.0),
            transaction_simulator=lambda _tx: _async_return(TransactionSimulationResult(ok=True)),
            circuit_breaker=breaker,
        )

        result = await execute_guarded_live_exit(
            settings=settings,
            mint_address="exit-mint",
            position_manager=manager,
            adapter=adapter,
            exit_transaction_builder=lambda _mint, _amount: _async_return("tx"),
            wallet_holdings_lookup=lambda: _wallet_holdings_for(manager, "exit-mint"),
            wallet_balance_lookup=lambda: _async_return(1.0),
            transaction_simulator=lambda _tx: _async_return(TransactionSimulationResult(ok=True)),
            circuit_breaker=breaker,
            env=_armed_env(settings),
        )

        assert result.ok is False
        assert result.diagnostics == ("readiness:circuit_breaker",)

    asyncio.run(run())


def test_fully_fake_ready_live_exit_can_close_existing_position(tmp_path: Path) -> None:
    async def run() -> None:
        settings = _live_settings()
        manager = await _seed_position_manager(tmp_path / "success.db", max_single_position_sol=0.01)
        submitter = RecordingRpcSubmitter()
        breaker = LiveCircuitBreaker()
        breaker.record_health_check(True)
        adapter = JupiterLiveExecutionAdapter(
            rpc_submitter=submitter,
            settings=settings,
            guardrail_env=_armed_env(settings),
            wallet_balance_lookup=lambda: _async_return(1.0),
            transaction_simulator=lambda _tx: _async_return(TransactionSimulationResult(ok=True)),
            circuit_breaker=breaker,
        )

        result = await execute_guarded_live_exit(
            settings=settings,
            mint_address="exit-mint",
            position_manager=manager,
            adapter=adapter,
            exit_transaction_builder=lambda mint, amount: _async_return(f"sell:{mint}:{amount}"),
            wallet_holdings_lookup=lambda: _wallet_holdings_for(manager, "exit-mint"),
            wallet_balance_lookup=lambda: _async_return(1.0),
            transaction_simulator=lambda _tx: _async_return(TransactionSimulationResult(ok=True)),
            circuit_breaker=breaker,
            env=_armed_env(settings),
        )

        assert result.ok is True
        assert result.diagnostics == ("live_exit_submitted",)
        assert result.provider == "rpc"
        assert submitter.calls and str(submitter.calls[0]).startswith("sell:exit-mint:")
        assert await manager.get_position("exit-mint") is None

    asyncio.run(run())


def test_live_exit_cli_fails_closed_by_default() -> None:
    result = runner.invoke(cli_module.app, ["live-exit", "--mint", "missing-mint"])

    assert result.exit_code == 0
    assert "Preflight Explainer" in result.stdout
    assert "BLOCKED" in result.stdout


async def _async_return(value):
    return value
