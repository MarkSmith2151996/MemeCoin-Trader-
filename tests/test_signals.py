import asyncio

from src.core.models import Signal, SignalSource as SignalSourceEnum, SignalType
from src.signals.aggregator import SignalAggregator
from src.signals.base import SignalSource


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
