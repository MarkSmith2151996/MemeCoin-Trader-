"""Coverage: circuit breaker + kill switch safety controls (MT-546).

Uses fake adapters/clients and tmp files — no real network, wallet keys, or
production state.
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid

import pytest

from src.chain.jupiter import SOL_MINT
from src.chain.jupiter_swap import JupiterSwapQuote, JupiterSwapResult
from src.core.config import load_settings
from src.core.database import init_db
from src.core.models import Position, PositionStatus, Side, Signal, SignalSource, SignalType, Trade
from src.execution.live import LiveExecutionAdapter
from src.execution.safety_controls import (
    MANUAL_RESET_CONFIRMATION,
    CircuitBreaker,
    KillSwitch,
    KillSwitchNotArmedError,
    V2KillSwitch,
    read_execution_mode,
    set_execution_mode,
)
from src.strategy.position_manager import PositionManager

TOKEN_MINT = "tok12345678901234567890123456789012"
SECOND_MINT = "tok12345678901234567890123456789013"


# ── fakes ────────────────────────────────────────────────────────────

class FakeSwapClient:
    def __init__(
        self,
        *,
        decimals: int = 6,
        sol_balance: float = 5.0,
        token_balance: float = 1000.0,
        quote_impact_pct: float = 0.01,
        swap_ok: bool = True,
        crash: bool = False,
    ) -> None:
        self.decimals = decimals
        self.sol_balance = sol_balance
        self.token_balance = token_balance
        self.quote_impact_pct = quote_impact_pct
        self.swap_ok = swap_ok
        self.crash = crash
        self.swap_calls: list[JupiterSwapQuote] = []

    async def get_token_decimals(self, mint: str) -> int:
        return self.decimals

    async def get_quote(self, input_mint, output_mint, amount_lamports, slippage_bps=100):
        return JupiterSwapQuote(
            input_mint=input_mint,
            output_mint=output_mint,
            in_amount=amount_lamports,
            out_amount=1_000_000,
            price_impact_pct=self.quote_impact_pct,
            slippage_bps=slippage_bps,
            token_decimals=self.decimals,
            price_sol=0.05,
            raw={"inAmount": str(amount_lamports), "outAmount": "1000000"},
        )

    async def execute_swap(self, quote: JupiterSwapQuote) -> JupiterSwapResult:
        self.swap_calls.append(quote)
        if self.crash:
            raise RuntimeError("rpc exploded")
        if not self.swap_ok:
            return JupiterSwapResult(
                ok=False,
                signature="sig-expired",
                input_mint=quote.input_mint,
                output_mint=quote.output_mint,
                in_amount=quote.in_amount,
                out_amount=quote.out_amount,
                price_sol=quote.price_sol,
                fees_lamports=5000,
                confirmation_status="expired",
                slot=None,
                attempts=3,
                error="blockhash expired before confirmation",
            )
        if quote.input_mint == SOL_MINT:
            self.token_balance += quote.out_amount / (10**quote.token_decimals)
        else:
            self.token_balance = 0.0
            self.sol_balance += quote.out_amount / 1_000_000_000
        return JupiterSwapResult(
            ok=True,
            signature="sig-abc",
            input_mint=quote.input_mint,
            output_mint=quote.output_mint,
            in_amount=quote.in_amount,
            out_amount=quote.out_amount,
            price_sol=quote.price_sol,
            fees_lamports=5000,
            confirmation_status="confirmed",
            slot=77,
            attempts=1,
            token_balance_after=(
                self.token_balance if quote.output_mint != SOL_MINT else self.sol_balance
            ),
        )

    async def get_sol_balance(self) -> float | None:
        return self.sol_balance

    async def get_token_balance(self, mint: str) -> float | None:
        return self.token_balance

    async def close(self) -> None:
        pass


class FakeSellAdapter:
    mode = "live"

    def __init__(self, *, fail_mints: set[str] | None = None) -> None:
        self.fail_mints = set(fail_mints or ())
        self.calls: list[tuple[str, float, int]] = []

    async def sell(self, mint_address: str, token_amount: float, slippage_bps: int = 100) -> Trade:
        self.calls.append((mint_address, token_amount, slippage_bps))
        if mint_address in self.fail_mints:
            raise RuntimeError(f"swap failed for {mint_address[:16]}")
        return Trade(
            mint_address=mint_address,
            side=Side.SELL,
            amount_sol=token_amount * 0.0002,
            token_amount=token_amount,
            price_sol=0.0002,
            slippage_bps=slippage_bps,
            tx_signature="sig-ks",
            mode="live",
            status="confirmed",
        )


class FakePaperSellAdapter(FakeSellAdapter):
    mode = "paper"


class FakeV2SellAdapter(FakeSellAdapter):
    def __init__(self, *, fail_mints: set[str] | None = None) -> None:
        super().__init__(fail_mints=fail_mints)
        self.cleared: list[str] = []

    async def verify_token_balance_cleared(self, mint_address: str) -> float:
        self.cleared.append(mint_address)
        return 0.0


class FakeHiveStore:
    def __init__(self, positions: list[dict[str, object]]) -> None:
        self.positions = positions
        self.closed: list[dict[str, object]] = []

    async def list_open_live_positions(self) -> list[dict[str, object]]:
        return self.positions

    async def close_position(self, position, trade, **values) -> None:
        self.closed.append({"position": position, "trade": trade, **values})


class FakeAlerts:
    def __init__(self) -> None:
        self.messages: list[tuple[str, str, str]] = []

    async def send(self, level: str, title: str, message: str) -> None:
        self.messages.append((level, title, message))


def _live_trade(mint: str, token_amount: float = 1000.0) -> Trade:
    return Trade(
        mint_address=mint,
        side=Side.BUY,
        amount_sol=0.05,
        token_amount=token_amount,
        price_sol=0.00005,
        slippage_bps=100,
        tx_signature="sig-buy",
        mode="live",
        status="confirmed",
    )


def _signal(mint: str) -> Signal:
    return Signal(
        source=SignalSource.MANUAL,
        type=SignalType.NEW_POOL,
        mint_address=mint,
        confidence=1.0,
    )


# ── circuit breaker ──────────────────────────────────────────────────

def test_breaker_starts_clear(tmp_path) -> None:
    breaker = CircuitBreaker(flag_path=tmp_path / "cb.json")
    assert breaker.is_tripped() is False
    assert breaker.status().tripped is False


def test_breaker_trip_writes_flag_and_logs_critical(tmp_path, caplog) -> None:
    flag = tmp_path / "cb.json"
    breaker = CircuitBreaker(flag_path=flag)
    with caplog.at_level(logging.CRITICAL, logger="safety_controls"):
        state = breaker.trip(mint=TOKEN_MINT, signature_attempt="sig-xyz", error="swap rejected")

    assert state.tripped is True
    assert state.reason == "sell_failure"
    assert state.mint == TOKEN_MINT
    assert state.signature_attempt == "sig-xyz"
    assert breaker.is_tripped() is True
    assert flag.is_file()
    data = json.loads(flag.read_text())
    assert data["tripped"] is True
    assert data["mint"] == TOKEN_MINT
    assert "sig-xyz" in data["signature_attempt"]
    assert any(
        "CIRCUIT BREAKER TRIPPED" in r.message and TOKEN_MINT in r.message
        for r in caplog.records
    )


def test_breaker_trip_preserves_original_reason(tmp_path, caplog) -> None:
    breaker = CircuitBreaker(flag_path=tmp_path / "cb.json")
    breaker.trip(mint=TOKEN_MINT, error="first failure")
    state = breaker.trip(mint=SECOND_MINT, error="second failure")

    assert state.mint == TOKEN_MINT
    assert state.error == "first failure"
    assert breaker.status().mint == TOKEN_MINT


def test_breaker_refreshes_cooldown_when_another_error_occurs(tmp_path) -> None:
    flag = tmp_path / "cb.json"
    breaker = CircuitBreaker(flag_path=flag)
    breaker.trip(mint=TOKEN_MINT, error="first failure")
    first = json.loads(flag.read_text())
    breaker.trip(mint=SECOND_MINT, error="second failure")
    second = json.loads(flag.read_text())

    assert second["mint"] == TOKEN_MINT
    assert second["error"] == "first failure"
    assert second["last_error_at"] >= first["last_error_at"]


def test_breaker_remains_tripped_after_elapsed_cooldown(tmp_path) -> None:
    flag = tmp_path / "cb.json"
    breaker = CircuitBreaker(flag_path=flag)
    breaker.trip(mint=TOKEN_MINT, error="boom")
    payload = json.loads(flag.read_text())
    payload["tripped_at"] = "2020-01-01T00:00:00+00:00"
    payload["last_error_at"] = "2020-01-01T00:00:00+00:00"
    flag.write_text(json.dumps(payload))

    assert breaker.is_tripped() is True
    assert flag.exists()


def test_breaker_reset_clears_flag(tmp_path) -> None:
    breaker = CircuitBreaker(flag_path=tmp_path / "cb.json")
    breaker.trip(mint=TOKEN_MINT, error="boom")
    assert breaker.is_tripped() is True

    with pytest.raises(ValueError, match="MANUAL_RESET"):
        breaker.reset(confirm="no")
    before = breaker.reset(confirm=MANUAL_RESET_CONFIRMATION)
    assert before.tripped is True
    assert breaker.is_tripped() is False
    assert not (tmp_path / "cb.json").exists()


def test_breaker_corrupt_flag_trips_in_live_mode(tmp_path) -> None:
    flag = tmp_path / "cb.json"
    flag.write_text("not json {{{")
    breaker = CircuitBreaker(flag_path=flag)
    assert breaker.is_tripped() is True
    assert breaker.status().reason == "breaker_state_corrupt"


def test_breaker_unreadable_flag_trips_in_live_mode(tmp_path, monkeypatch) -> None:
    flag = tmp_path / "cb.json"
    flag.write_text("{}")
    breaker = CircuitBreaker(flag_path=flag)

    def unreadable(_self, **_kwargs):
        raise OSError("permission denied")

    monkeypatch.setattr(type(flag), "read_text", unreadable)
    state = breaker.status()

    assert state.tripped is True
    assert state.reason == "breaker_state_unreadable"


def test_breaker_unreadable_flag_stays_clear_in_paper_mode(tmp_path, monkeypatch) -> None:
    flag = tmp_path / "cb.json"
    flag.write_text("{}")
    breaker = CircuitBreaker(flag_path=flag, execution_mode="paper")

    def unreadable(_self, **_kwargs):
        raise OSError("permission denied")

    monkeypatch.setattr(type(flag), "read_text", unreadable)
    assert breaker.is_tripped() is False


# ── live adapter integration ─────────────────────────────────────────

def test_live_buy_blocked_when_breaker_tripped(tmp_path) -> None:
    breaker = CircuitBreaker(flag_path=tmp_path / "cb.json")
    breaker.trip(mint=TOKEN_MINT, error="prior sell failure")
    client = FakeSwapClient()
    adapter = LiveExecutionAdapter(client=client, circuit_breaker=breaker)

    with pytest.raises(RuntimeError, match="circuit breaker tripped"):
        asyncio.run(adapter.buy(TOKEN_MINT, 0.05))
    assert client.swap_calls == []


def test_live_buy_proceeds_when_breaker_clear(tmp_path) -> None:
    breaker = CircuitBreaker(flag_path=tmp_path / "cb.json")
    client = FakeSwapClient()
    adapter = LiveExecutionAdapter(client=client, circuit_breaker=breaker)

    trade = asyncio.run(adapter.buy(TOKEN_MINT, 0.05))

    assert trade.side == Side.BUY
    assert len(client.swap_calls) == 1


def test_live_sell_failure_trips_breaker(tmp_path) -> None:
    breaker = CircuitBreaker(flag_path=tmp_path / "cb.json")
    adapter = LiveExecutionAdapter(client=FakeSwapClient(swap_ok=False), circuit_breaker=breaker)

    with pytest.raises(RuntimeError, match="live sell failed"):
        asyncio.run(adapter.sell(TOKEN_MINT, 100.0))

    state = breaker.status()
    assert state.tripped is True
    assert state.mint == TOKEN_MINT
    assert state.signature_attempt == "sig-expired"
    assert "expired" in (state.error or "")


def test_live_sell_crash_trips_breaker(tmp_path) -> None:
    breaker = CircuitBreaker(flag_path=tmp_path / "cb.json")
    adapter = LiveExecutionAdapter(client=FakeSwapClient(crash=True), circuit_breaker=breaker)

    with pytest.raises(RuntimeError, match="rpc exploded"):
        asyncio.run(adapter.sell(TOKEN_MINT, 100.0))

    state = breaker.status()
    assert state.tripped is True
    assert "rpc exploded" in (state.error or "")


def test_live_sell_success_does_not_trip(tmp_path) -> None:
    breaker = CircuitBreaker(flag_path=tmp_path / "cb.json")
    adapter = LiveExecutionAdapter(client=FakeSwapClient(), circuit_breaker=breaker)

    trade = asyncio.run(adapter.sell(TOKEN_MINT, 100.0))

    assert trade.side == Side.SELL
    assert breaker.is_tripped() is False


def test_live_exits_still_fire_while_tripped(tmp_path) -> None:
    breaker = CircuitBreaker(flag_path=tmp_path / "cb.json")
    breaker.trip(mint=TOKEN_MINT, error="earlier failure")
    client = FakeSwapClient()
    adapter = LiveExecutionAdapter(client=client, circuit_breaker=breaker)

    trade = asyncio.run(adapter.sell(TOKEN_MINT, 100.0))

    assert trade.side == Side.SELL
    assert len(client.swap_calls) == 1


# ── env helpers ──────────────────────────────────────────────────────

def test_read_execution_mode_defaults_paper(tmp_path) -> None:
    assert read_execution_mode(tmp_path / "missing.env") == "paper"


def test_set_execution_mode_replaces_and_preserves(tmp_path) -> None:
    env = tmp_path / ".env"
    env.write_text("HELIUS_API_KEY=abc\nEXECUTION_MODE=live\nMAX_POSITION_SOL=1\n")

    set_execution_mode(env, "paper")

    text = env.read_text()
    assert "HELIUS_API_KEY=abc" in text
    assert "MAX_POSITION_SOL=1" in text
    assert "EXECUTION_MODE=paper" in text
    assert "EXECUTION_MODE=live" not in text
    assert read_execution_mode(env) == "paper"


def test_set_execution_mode_appends_when_absent(tmp_path) -> None:
    env = tmp_path / ".env"
    env.write_text("HELIUS_API_KEY=abc\n")

    set_execution_mode(env, "paper")

    assert read_execution_mode(env) == "paper"
    assert env.read_text().endswith("EXECUTION_MODE=paper\n")


# ── kill switch ──────────────────────────────────────────────────────

def test_kill_switch_refuses_when_not_live(tmp_path) -> None:
    env = tmp_path / ".env"
    env.write_text("EXECUTION_MODE=paper\n")
    kill_switch = KillSwitch(env_path=env, adapter=FakeSellAdapter())

    with pytest.raises(KillSwitchNotArmedError, match="live-mode only"):
        asyncio.run(kill_switch.run())


def test_kill_switch_refuses_paper_adapter_even_when_env_is_live(tmp_path) -> None:
    env = tmp_path / ".env"
    env.write_text("EXECUTION_MODE=live\n")
    kill_switch = KillSwitch(env_path=env, adapter=FakePaperSellAdapter())

    with pytest.raises(RuntimeError, match="live execution adapter"):
        asyncio.run(kill_switch.run())

    assert read_execution_mode(env) == "live"


def test_kill_switch_sets_paper_and_liquidates(tmp_path) -> None:
    env = tmp_path / ".env"
    env.write_text("HELIUS_API_KEY=abc\nEXECUTION_MODE=live\n")
    breaker = CircuitBreaker(flag_path=tmp_path / "cb.json")
    adapter = FakeSellAdapter()
    kill_switch = KillSwitch(env_path=env, breaker=breaker, adapter=adapter)

    positions = [
        Position(
            mint_address=TOKEN_MINT, entry_trade_id=str(uuid.uuid4()),
            amount_sol=0.05, token_amount=1000.0, entry_price_sol=0.00005,
            mode='live',
        ),
        Position(
            mint_address=SECOND_MINT, entry_trade_id=str(uuid.uuid4()),
            amount_sol=0.05, token_amount=2000.0, entry_price_sol=0.000025,
            mode='live',
        ),
    ]
    summary = asyncio.run(kill_switch.run(positions=positions))

    assert summary.mode_before == "live"
    assert summary.mode_after == "paper"
    assert summary.breaker_tripped is True
    assert summary.positions_found == 2
    assert summary.sold == 2
    assert summary.failed == 0
    assert read_execution_mode(env) == "paper"
    state = breaker.status()
    assert state.tripped is True
    assert state.reason == "kill_switch"
    assert len(adapter.calls) == 2
    assert all(slippage == 500 for (_, _, slippage) in adapter.calls)
    assert breaker.is_tripped() is True


def test_kill_switch_counts_sell_failures(tmp_path) -> None:
    env = tmp_path / ".env"
    env.write_text("EXECUTION_MODE=live\n")
    adapter = FakeSellAdapter(fail_mints={TOKEN_MINT})
    breaker = CircuitBreaker(flag_path=tmp_path / "cb.json")
    kill_switch = KillSwitch(env_path=env, breaker=breaker, adapter=adapter)

    positions = [
        Position(
            mint_address=TOKEN_MINT, entry_trade_id=str(uuid.uuid4()),
            amount_sol=0.05, token_amount=1000.0, entry_price_sol=0.00005,
            mode='live',
        ),
        Position(
            mint_address=SECOND_MINT, entry_trade_id=str(uuid.uuid4()),
            amount_sol=0.05, token_amount=2000.0, entry_price_sol=0.000025,
            mode='live',
        ),
    ]
    summary = asyncio.run(kill_switch.run(positions=positions))

    assert summary.sold == 1
    assert summary.failed == 1
    assert read_execution_mode(env) == "paper"
    assert any(detail.startswith("FAIL") for detail in summary.details)
    assert any(detail.startswith("OK") for detail in summary.details)


def test_kill_switch_closes_positions_and_records_trades(tmp_path) -> None:
    db_path = tmp_path / "trades.db"
    asyncio.run(init_db(db_path))
    manager = PositionManager(db_path, load_settings(), strategy="B")
    asyncio.run(manager.open_position(_live_trade(TOKEN_MINT), _signal(TOKEN_MINT)))
    assert len(asyncio.run(manager.get_all_open(mode="live"))) == 1

    env = tmp_path / ".env"
    env.write_text("EXECUTION_MODE=live\n")
    kill_switch = KillSwitch(
        env_path=env,
        breaker=CircuitBreaker(flag_path=tmp_path / "cb.json"),
        adapter=FakeSellAdapter(),
    )
    summary = asyncio.run(
        kill_switch.run(
            positions=asyncio.run(manager.get_all_open(mode="live")),
            db_path=db_path,
            manager=manager,
        ),
    )

    assert summary.sold == 1
    assert asyncio.run(manager.get_all_open(mode="live")) == []
    # closed positions are not returned as open
    assert asyncio.run(manager.get_position(TOKEN_MINT, mode="live")) is None
    import aiosqlite

    async def _read() -> str | None:
        async with aiosqlite.connect(db_path) as conn:
            cursor = await conn.execute(
                "SELECT partial_exits_json FROM positions"
                " WHERE mint_address = ? AND strategy = 'B'",
                (TOKEN_MINT,),
            )
            row = await cursor.fetchone()
        return row[0] if row else None

    raw = asyncio.run(_read())
    assert raw is not None
    persisted = Position.model_validate_json(raw)
    assert persisted.status == PositionStatus.CLOSED
    assert persisted.mode == "live"
    assert persisted.close_price_sol == pytest.approx(0.0002)


def test_v2_kill_switch_closes_every_open_live_hive_position(tmp_path) -> None:
    async def run() -> None:
        env = tmp_path / ".env"
        env.write_text("EXECUTION_MODE=live\nLIVE_KILL_SWITCH=false\n")
        positions = [
            {
                "id": "position-a",
                "mint_address": TOKEN_MINT,
                "token_amount": 1000.0,
                "amount_sol": 0.05,
                "strategy": "BT",
            },
            {
                "id": "position-b",
                "mint_address": SECOND_MINT,
                "token_amount": 2000.0,
                "amount_sol": 0.05,
                "strategy": "other-live-strategy",
            },
        ]
        store = FakeHiveStore(positions)
        adapter = FakeV2SellAdapter()
        breaker = CircuitBreaker(flag_path=tmp_path / "breaker.json")
        kill_switch = V2KillSwitch(
            store=store,
            adapter=adapter,
            env_path=env,
            breaker=breaker,
            halt_path=tmp_path / "halt",
        )

        summary = await kill_switch.run()

        assert summary.positions_found == 2
        assert summary.sold == 2
        assert summary.failed == 0
        assert [entry["position"]["mint_address"] for entry in store.closed] == [
            TOKEN_MINT,
            SECOND_MINT,
        ]
        assert adapter.cleared == [TOKEN_MINT, SECOND_MINT]
        assert all(entry["close_reason"] == "kill_switch" for entry in store.closed)
        assert "EXECUTION_MODE=paper" in env.read_text()
        assert "LIVE_KILL_SWITCH=true" in env.read_text()
        assert breaker.status().reason == "kill_switch"
        assert (tmp_path / "halt").exists()

    asyncio.run(run())


def test_v2_kill_switch_alerts_and_leaves_failed_positions_open(tmp_path) -> None:
    async def run() -> None:
        env = tmp_path / ".env"
        env.write_text("EXECUTION_MODE=live\n")
        store = FakeHiveStore(
            [
                {
                    "id": "position-a",
                    "mint_address": TOKEN_MINT,
                    "token_amount": 1000.0,
                    "amount_sol": 0.05,
                },
            ],
        )
        alerts = FakeAlerts()
        kill_switch = V2KillSwitch(
            store=store,
            adapter=FakeV2SellAdapter(fail_mints={TOKEN_MINT}),
            env_path=env,
            breaker=CircuitBreaker(flag_path=tmp_path / "breaker.json"),
            halt_path=tmp_path / "halt",
            alert_manager=alerts,
        )

        summary = await kill_switch.run()

        assert summary.sold == 0
        assert summary.failed == 1
        assert store.closed == []
        assert alerts.messages[0][1] == "V2 kill switch incomplete"
        assert TOKEN_MINT[:16] in alerts.messages[0][2]

    asyncio.run(run())
