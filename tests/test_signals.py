import asyncio
from pathlib import Path

from src.core.models import Signal, SignalSource as SignalSourceEnum, SignalType
from src.signals.aggregator import SignalAggregator
from src.signals.base import SignalSource
from src.signals.pump_fun import PumpFunMonitor
from src.signals.whale_tracker import WhaleWalletTracker


class DummySource(SignalSource):
    @property
    def name(self) -> str:
        return "dummy"

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def poll(self) -> list[Signal]:
        return [
            Signal(
                source=SignalSourceEnum.MANUAL,
                type=SignalType.NEW_POOL,
                mint_address="mint",
                confidence=0.8,
            )
        ]


def test_signal_aggregator_flattens_source_batches() -> None:
    async def run() -> list[Signal]:
        aggregator = SignalAggregator([DummySource()])
        return await aggregator.poll()

    signals = asyncio.run(run())

    assert len(signals) == 1
    assert signals[0].mint_address == "mint"


def test_pump_fun_monitor_normalizes_http_events_and_dedupes() -> None:
    responses = [
        [
            {"mint": "mint-1", "symbol": "AAA", "signature": "sig-1"},
            {
                "mintAddress": "mint-2",
                "eventType": "migration",
                "name": "Token Two",
                "eventId": "evt-2",
            },
            {"symbol": "missing-mint"},
        ],
        [
            {"mint": "mint-1", "symbol": "AAA", "signature": "sig-1"},
            {
                "mintAddress": "mint-2",
                "eventType": "migration",
                "name": "Token Two",
                "eventId": "evt-2",
            },
        ],
    ]

    async def fake_fetcher(_client: object, _url: str) -> object:
        return responses.pop(0)

    async def run() -> tuple[list[Signal], list[Signal]]:
        monitor = PumpFunMonitor(
            http_urls=("https://example.invalid/latest",),
            websocket_url=None,
            poll_interval_s=0.0,
            http_fetcher=fake_fetcher,
        )
        first = await monitor.poll()
        second = await monitor.poll()
        await monitor.stop()
        return first, second

    first_batch, second_batch = asyncio.run(run())

    assert [signal.mint_address for signal in first_batch] == ["mint-1", "mint-2"]
    assert first_batch[0].type == SignalType.NEW_POOL
    assert first_batch[0].payload["signature"] == "sig-1"
    assert first_batch[1].type == SignalType.GRADUATION
    assert first_batch[1].confidence == 0.95
    assert second_batch == []


def test_pump_fun_monitor_prefers_nested_coin_mint_address() -> None:
    async def fake_fetcher(_client: object, _url: str) -> object:
        return {
            "data": [
                {
                    "event": "new_token",
                    "coin": {"mint": "nested-mint"},
                    "ticker": "NEST",
                }
            ]
        }

    async def run() -> list[Signal]:
        monitor = PumpFunMonitor(
            http_urls=("https://example.invalid/latest",),
            websocket_url=None,
            http_fetcher=fake_fetcher,
        )
        signals = await monitor.poll()
        await monitor.stop()
        return signals

    signals = asyncio.run(run())

    assert len(signals) == 1
    assert signals[0].mint_address == "nested-mint"
    assert signals[0].message == "pump.fun new_pool for NEST"


def test_whale_tracker_skips_placeholder_wallets(tmp_path: Path) -> None:
    config_path = tmp_path / "wallets.yaml"
    config_path.write_text(
        """
wallets:
  - address: "<placeholder_wallet_1>"
    label: "placeholder"
    tier: "S"
    enabled: true
""".strip(),
        encoding="utf-8",
    )

    tracker = WhaleWalletTracker(wallets_config_path=config_path, api_key="test-key")

    async def run() -> None:
        await tracker.start()
        try:
            assert await tracker.poll() == []
        finally:
            await tracker.stop()

    asyncio.run(run())


def test_whale_tracker_emits_buy_signal_and_deduplicates(tmp_path: Path) -> None:
    config_path = tmp_path / "wallets.yaml"
    config_path.write_text(
        """
wallets:
  - address: "wallet-1"
    label: "alpha"
    tier: "S"
    enabled: true
""".strip(),
        encoding="utf-8",
    )

    class StubWhaleWalletTracker(WhaleWalletTracker):
        async def _fetch_transactions(self, wallet_address: str) -> list[dict[str, object]]:
            assert wallet_address == "wallet-1"
            return [
                {
                    "signature": "sig-1",
                    "tokenTransfers": [
                        {
                            "mint": "mint-1",
                            "toUserAccount": "wallet-1",
                            "fromUserAccount": "market-maker",
                            "tokenAmount": 25000,
                        }
                    ],
                }
            ]

    tracker = StubWhaleWalletTracker(wallets_config_path=config_path, api_key="test-key")

    async def run() -> None:
        await tracker.start()
        try:
            first_batch = await tracker.poll()
            second_batch = await tracker.poll()
        finally:
            await tracker.stop()

        assert len(first_batch) == 1
        assert second_batch == []
        assert first_batch[0].source == SignalSourceEnum.WHALE_TRACKER
        assert first_batch[0].type == SignalType.BUY
        assert first_batch[0].mint_address == "mint-1"
        assert first_batch[0].confidence > 0.0
        assert first_batch[0].payload["tracked_wallet"]["label"] == "alpha"

    asyncio.run(run())
