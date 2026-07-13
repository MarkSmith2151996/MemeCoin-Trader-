import asyncio
import inspect
from datetime import UTC, datetime, timedelta

from src.core.models import SignalSource as SignalSourceEnum, SignalType
from src.signals.base import SignalSource
from src.signals.onchain import OnChainMonitor, PairSnapshot


def _snapshot(
    *,
    mint_address: str = "mint-1",
    volume_m5: float = 0.0,
    volume_h1: float = 0.0,
    volume_h24: float = 0.0,
    buys_m5: float = 0.0,
    sells_m5: float = 0.0,
    liquidity_usd: float = 0.0,
    holder_count: int | None = None,
    created_minutes_ago: int | None = None,
) -> PairSnapshot:
    created_at = None
    if created_minutes_ago is not None:
        created_at = datetime.now(UTC) - timedelta(minutes=created_minutes_ago)
    return PairSnapshot(
        mint_address=mint_address,
        pair_address=f"pair-{mint_address}",
        dex_id="raydium",
        symbol="MEME",
        volume_m5=volume_m5,
        volume_h1=volume_h1,
        volume_h24=volume_h24,
        buys_m5=buys_m5,
        sells_m5=sells_m5,
        liquidity_usd=liquidity_usd,
        holder_count=holder_count,
        pair_created_at=created_at,
    )


def test_onchain_monitor_implements_signal_source_interface() -> None:
    class InterfaceMonitor(OnChainMonitor):
        async def _fetch_candidate_mints(self) -> list[str]:
            return []

    monitor = InterfaceMonitor()

    assert isinstance(monitor, SignalSource)
    assert monitor.name == "onchain"
    assert asyncio.run(monitor.poll()) == []


def test_onchain_monitor_volume_spike_signal_strength_scales() -> None:
    monitor = OnChainMonitor()
    previous = _snapshot(volume_m5=100.0, buys_m5=1.0, sells_m5=2.0, liquidity_usd=10_000.0)
    current = _snapshot(volume_m5=500.0, buys_m5=2.0, sells_m5=2.0, liquidity_usd=10_000.0)

    signal = monitor._build_signal(current, previous)

    assert signal is not None
    assert signal.source == SignalSourceEnum.ONCHAIN
    assert signal.type == SignalType.VOLUME_SPIKE
    assert signal.confidence == 0.6
    assert signal.payload["metrics"]["volume_score"] == 0.6


def test_onchain_monitor_buy_sell_ratio_scales_momentum() -> None:
    monitor = OnChainMonitor()

    assert monitor._score_buy_sell_ratio(3.0, 2.0) == 0.3
    assert monitor._score_buy_sell_ratio(15.0, 5.0) == 0.6
    assert monitor._score_buy_sell_ratio(25.0, 5.0) == 0.9


def test_onchain_monitor_provider_failures_degrade_gracefully() -> None:
    class FailingMonitor(OnChainMonitor):
        async def _fetch_candidate_mints(self) -> list[str]:
            return ["mint-1"]

        async def _fetch_snapshot(self, mint_address: str) -> PairSnapshot | None:
            assert mint_address == "mint-1"
            return None

    assert asyncio.run(FailingMonitor().poll()) == []


def test_onchain_monitor_preserves_valid_pair_age_provenance() -> None:
    monitor = OnChainMonitor()
    snapshot = _snapshot(
        volume_m5=500.0,
        volume_h1=1_000.0,
        buys_m5=3.0,
        sells_m5=1.0,
        liquidity_usd=10_000.0,
        created_minutes_ago=10,
    )

    signal = monitor._build_signal(snapshot, previous=None)

    assert signal is not None
    assert signal.payload["provider"] == "dexscreener"
    assert signal.payload["pair_created_at"] == snapshot.pair_created_at.isoformat()


def test_onchain_monitor_rejects_zero_and_negative_pair_timestamps() -> None:
    monitor = OnChainMonitor()

    assert monitor._extract_timestamp(0) is None
    assert monitor._extract_timestamp(-1) is None


def test_onchain_monitor_deduplicates_by_mint_within_poll_window() -> None:
    class StubOnChainMonitor(OnChainMonitor):
        def __init__(self) -> None:
            super().__init__()
            self._poll_count = 0

        async def _fetch_candidate_mints(self) -> list[str]:
            return ["mint-1"]

        async def _fetch_snapshot(self, mint_address: str) -> PairSnapshot | None:
            assert mint_address == "mint-1"
            self._poll_count += 1
            if self._poll_count == 1:
                return _snapshot(
                    mint_address=mint_address,
                    volume_m5=400.0,
                    volume_h1=2400.0,
                    buys_m5=2.0,
                    sells_m5=2.0,
                    liquidity_usd=10_000.0,
                )
            return _snapshot(
                mint_address=mint_address,
                volume_m5=1000.0,
                buys_m5=2.0,
                sells_m5=2.0,
                liquidity_usd=10_000.0,
            )

    async def run() -> tuple[list, list]:
        monitor = StubOnChainMonitor()
        first_batch = await monitor.poll()
        second_batch = await monitor.poll()
        return first_batch, second_batch

    first_batch, second_batch = asyncio.run(run())

    assert len(first_batch) == 1
    assert first_batch[0].mint_address == "mint-1"
    assert second_batch == []


def test_onchain_monitor_has_no_wallet_or_live_trade_code() -> None:
    source = inspect.getsource(OnChainMonitor).lower()

    assert "private_key" not in source
    assert "send_transaction" not in source
    assert "swap" not in source
    assert "src.chain.wallet" not in source
