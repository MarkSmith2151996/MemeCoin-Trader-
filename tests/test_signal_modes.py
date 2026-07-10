from datetime import UTC, datetime

from src.core.models import Signal, SignalSource as SignalSourceEnum, SignalType
from src.signals.modes import CandidateMode, classify_candidate_mode


BASE_TIME = datetime(2026, 7, 10, tzinfo=UTC)


def test_pump_fun_create_signal_is_classified_as_launch() -> None:
    signal = Signal(
        source=SignalSourceEnum.PUMP_FUN,
        type=SignalType.NEW_POOL,
        mint_address="launch-mint",
        observed_at=BASE_TIME,
        payload={"txType": "create", "pool": "pump", "name": "Launch"},
    )

    assert classify_candidate_mode(signal) == CandidateMode.LAUNCH


def test_pump_fun_migration_signal_is_classified_as_migration() -> None:
    signal = Signal(
        source=SignalSourceEnum.PUMP_FUN,
        type=SignalType.GRADUATION,
        mint_address="migration-mint",
        observed_at=BASE_TIME,
        payload={"txType": "migrate", "pool": "raydium", "symbol": "GRAD"},
    )

    assert classify_candidate_mode(signal) == CandidateMode.MIGRATION


def test_normal_non_pump_signal_without_clear_context_is_unknown() -> None:
    signal = Signal(
        source=SignalSourceEnum.ONCHAIN,
        type=SignalType.BUY,
        mint_address="unknown-mint",
        observed_at=BASE_TIME,
        payload={"symbol": "WATCH"},
    )

    assert classify_candidate_mode(signal) == CandidateMode.UNKNOWN


def test_missing_or_malformed_payload_degrades_to_unknown() -> None:
    malformed = {"source": "PUMP_FUN", "type": None, "payload": "bad"}

    assert classify_candidate_mode(malformed) == CandidateMode.UNKNOWN


def test_classification_is_pure_and_does_not_change_signal_fields() -> None:
    signal = Signal(
        source=SignalSourceEnum.PUMP_FUN,
        type=SignalType.NEW_POOL,
        mint_address="stable-mint",
        confidence=0.85,
        weight=1.2,
        observed_at=BASE_TIME,
        payload={"txType": "create", "pool": "pump", "symbol": "SAFE"},
    )

    before = signal.model_dump(mode="json")

    result = classify_candidate_mode(signal)

    assert result == CandidateMode.LAUNCH
    assert signal.model_dump(mode="json") == before


def test_composite_raw_data_with_pump_fun_migration_is_classified_as_migration() -> None:
    composite = {
        "source": "ONCHAIN",
        "type": "BUY",
        "payload": {
            "raw_data": [
                {
                    "source": "PUMP_FUN",
                    "type": "GRADUATION",
                    "payload": {"txType": "migrate", "pool": "raydium"},
                },
                {
                    "source": "ONCHAIN",
                    "type": "BUY",
                    "payload": {"symbol": "COIN"},
                },
            ]
        },
    }

    assert classify_candidate_mode(composite) == CandidateMode.MIGRATION
