import json

from src.core.models import SignalSource as SignalSourceEnum, SignalType
from src.signals.pump_fun import PumpFunMonitor


def test_pump_fun_monitor_normalizes_live_create_payload_shape() -> None:
    payload = {
        "signature": "2Z89mEEGmLrkq9fLutMrP87a95CtruNrEZhSuWZU9wpL4LM8K2jg6zKNVN8BbvzMF4HShves14R4zrRhrGNaVqx5",
        "mint": "BN995hXFtcfq8J5C6Q49U2fSUHrXQWC5u9bvq6jGpump",
        "traderPublicKey": "G1n2rjAEXZ44TpDWL19NZNJ2SEnVg33RKbFxkEU25E4J",
        "txType": "create",
        "initialBuy": 3564784.088685,
        "solAmount": 0.100000001,
        "bondingCurveKey": "BwWK17cbHxwWBKZkUYvzxLcNQ1YVyaFezduWbtm2de6s",
        "vTokensInBondingCurve": 1069435215.911315,
        "vSolInBondingCurve": 30.100000000999984,
        "marketCapSol": 28.14569742342961,
        "name": "save the monkey ",
        "symbol": "YY Evil",
        "uri": "https://ipfs.io/ipfs/Qmf8WiNijqunfBEQubmY27fukejXoHVHDLuhCmL3iKaN6M",
        "is_mayhem_mode": True,
        "pool": "pump",
    }

    signal = PumpFunMonitor(websocket_url=None)._payload_to_signal(payload)

    assert signal is not None
    assert signal.source == SignalSourceEnum.PUMP_FUN
    assert signal.type == SignalType.NEW_POOL
    assert signal.mint_address == payload["mint"]
    assert signal.confidence == 0.85
    assert signal.message == "pump.fun new_pool for YY Evil"


def test_pump_fun_monitor_recognizes_graduation_from_live_fields() -> None:
    payload = {
        "mint": "4wTV6D5EwLx2KVAz5dWftP7vFvN6PVaZ2wM8VQV6pump",
        "txType": "migrate",
        "pool": "raydium",
        "symbol": "GRAD",
        "signature": "migration-sig-1",
    }

    signal = PumpFunMonitor(websocket_url=None)._payload_to_signal(payload)

    assert signal is not None
    assert signal.type == SignalType.GRADUATION
    assert signal.confidence == 0.95
    assert signal.message == "pump.fun graduation for GRAD"


def test_pump_fun_monitor_safely_handles_ack_and_malformed_websocket_messages() -> None:
    monitor = PumpFunMonitor(websocket_url=None)

    ack_payload = monitor._parse_websocket_message(
        json.dumps({"message": "Successfully subscribed to token creation events."})
    )
    malformed_payload = monitor._parse_websocket_message("not-json")

    assert ack_payload == {"message": "Successfully subscribed to token creation events."}
    assert monitor._payload_to_signal(ack_payload) is None
    assert malformed_payload is None


def test_pump_fun_monitor_rejects_obvious_junk_launch_identity() -> None:
    monitor = PumpFunMonitor(websocket_url=None)

    missing_identity_payload = {
        "mint": "Junk111111111111111111111111111111111111pump",
        "txType": "create",
        "pool": "pump",
        "name": "   ",
        "symbol": "   ",
        "signature": "junk-sig-1",
    }
    url_symbol_payload = {
        "mint": "Junk222222222222222222222222222222222222pump",
        "txType": "create",
        "pool": "pump",
        "name": "real enough",
        "symbol": "https://spam.example",
        "signature": "junk-sig-2",
    }
    punctuated_symbol_payload = {
        "mint": "Junk333333333333333333333333333333333333pump",
        "txType": "create",
        "pool": "pump",
        "name": "normal name",
        "symbol": "$-$",
        "signature": "junk-sig-3",
    }

    assert monitor._payload_to_signal(missing_identity_payload) is None
    assert monitor._payload_to_signal(url_symbol_payload) is None
    assert monitor._payload_to_signal(punctuated_symbol_payload) is None


def test_pump_fun_monitor_keeps_quirky_but_usable_identity() -> None:
    payload = {
        "mint": "Safe111111111111111111111111111111111111pump",
        "txType": "create",
        "pool": "pump",
        "name": "cat.wif.laser",
        "symbol": "LASR",
        "signature": "safe-sig-1",
    }

    signal = PumpFunMonitor(websocket_url=None)._payload_to_signal(payload)

    assert signal is not None
    assert signal.source == SignalSourceEnum.PUMP_FUN
    assert signal.type == SignalType.NEW_POOL
    assert signal.mint_address == payload["mint"]
