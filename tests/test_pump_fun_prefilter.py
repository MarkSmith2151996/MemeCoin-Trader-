from src.core.models import SignalType
from src.signals.pump_fun import PumpFunMonitor


class FakeClock:
    def __init__(self, start: float = 0.0) -> None:
        self.value = start

    def __call__(self) -> float:
        return self.value


def test_name_filter_blocks_test_and_airdrop_patterns() -> None:
    monitor = PumpFunMonitor(websocket_url=None)

    blocked_test = monitor._payload_to_signal(
        {
            "mint": "test-mint",
            "name": "TEST launch",
            "symbol": "OK",
        }
    )
    blocked_airdrop = monitor._payload_to_signal(
        {
            "mint": "airdrop-mint",
            "name": "Legit Name",
            "symbol": "AIRDROP",
        }
    )

    assert blocked_test is None
    assert blocked_airdrop is None


def test_creator_repeat_filter_blocks_fourth_launch_within_one_hour() -> None:
    clock = FakeClock()
    monitor = PumpFunMonitor(websocket_url=None, now_monotonic=clock)
    base_payload = {
        "name": "Normal Token",
        "symbol": "NORM",
        "creatorAddress": "creator-wallet",
    }

    first = monitor._payload_to_signal({**base_payload, "mint": "mint-1"})
    second = monitor._payload_to_signal({**base_payload, "mint": "mint-2"})
    third = monitor._payload_to_signal({**base_payload, "mint": "mint-3"})
    fourth = monitor._payload_to_signal({**base_payload, "mint": "mint-4"})

    assert first is not None
    assert second is not None
    assert third is not None
    assert fourth is None


def test_graduation_boost_applies_to_effective_signal_strength() -> None:
    monitor = PumpFunMonitor(websocket_url=None)
    payload = {
        "mint": "grad-mint",
        "txType": "migrate",
        "pool": "raydium",
        "symbol": "GRAD",
        "signature": "migration-sig-1",
    }

    signal = monitor._payload_to_signal(payload)

    assert signal is not None
    assert signal.type == SignalType.GRADUATION
    assert signal.confidence == 0.95
    assert round(min(signal.confidence * signal.weight, 1.0), 6) == 1.0
    assert signal.payload["raw_data"] == payload
