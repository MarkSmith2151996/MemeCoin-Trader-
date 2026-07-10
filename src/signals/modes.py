"""Deterministic candidate mode classification helpers."""

from __future__ import annotations

from enum import StrEnum
from typing import Any

from src.core.models import Signal, SignalSource, SignalType


class CandidateMode(StrEnum):
    LAUNCH = "launch"
    MIGRATION = "migration"
    UNKNOWN = "unknown"


def classify_candidate_mode(signal_or_opportunity: object) -> CandidateMode:
    records = list(_candidate_records(signal_or_opportunity))
    if not records:
        return CandidateMode.UNKNOWN

    if any(_is_migration_record(record) for record in records):
        return CandidateMode.MIGRATION
    if any(_is_launch_record(record) for record in records):
        return CandidateMode.LAUNCH
    return CandidateMode.UNKNOWN


def _candidate_records(signal_or_opportunity: object) -> tuple[dict[str, object], ...]:
    signal_record = _signal_like_record(signal_or_opportunity)
    if signal_record is None:
        return ()

    payload = signal_record.get("payload")
    if not isinstance(payload, dict):
        return (signal_record,)

    raw_data = payload.get("raw_data")
    nested_records: list[dict[str, object]] = []
    if isinstance(raw_data, list):
        for item in raw_data:
            nested = _signal_like_record(item)
            if nested is not None:
                nested_records.append(nested)

    return tuple([signal_record, *nested_records])


def _signal_like_record(value: object) -> dict[str, object] | None:
    if isinstance(value, Signal):
        return {
            "source": value.source.value,
            "type": value.type.value,
            "payload": value.payload,
        }
    if isinstance(value, dict):
        source = _stringish(value.get("source"))
        signal_type = _stringish(value.get("type"))
        payload = value.get("payload") if isinstance(value.get("payload"), dict) else value
        return {
            "source": source,
            "type": signal_type,
            "payload": payload,
        }
    return None


def _is_migration_record(record: dict[str, object]) -> bool:
    payload = record.get("payload") if isinstance(record.get("payload"), dict) else {}
    source = _normalized_source(record.get("source"))
    signal_type = _normalized_type(record.get("type"))

    if signal_type == SignalType.GRADUATION:
        return True

    tx_type = _stringish(payload.get("txType"))
    pool = _stringish(payload.get("pool"))
    event_type = _stringish(payload.get("event")) or _stringish(payload.get("type"))
    stage_hint = _stringish(payload.get("stage_hint"))

    if tx_type and any(keyword in tx_type.lower() for keyword in ("migrate", "graduat")):
        return True
    if event_type and any(keyword in event_type.lower() for keyword in ("migrate", "graduat")):
        return True
    if stage_hint and any(keyword in stage_hint.lower() for keyword in ("migrat", "graduat")):
        return True
    if source == SignalSource.PUMP_FUN and pool and pool.lower() == "raydium":
        return True
    return False


def _is_launch_record(record: dict[str, object]) -> bool:
    payload = record.get("payload") if isinstance(record.get("payload"), dict) else {}
    source = _normalized_source(record.get("source"))
    signal_type = _normalized_type(record.get("type"))

    if source != SignalSource.PUMP_FUN:
        return False

    if signal_type == SignalType.NEW_POOL:
        return True

    tx_type = _stringish(payload.get("txType"))
    pool = _stringish(payload.get("pool"))
    stage_hint = _stringish(payload.get("stage_hint"))
    source_context = _stringish(payload.get("source_context_hint"))

    if tx_type and tx_type.lower() == "create":
        return True
    if pool and pool.lower() == "pump":
        return True
    if stage_hint and stage_hint.lower() == "new_pool":
        return True
    if source_context and source_context.lower() == "single-source-launch":
        return True
    return False


def _normalized_source(value: object) -> SignalSource | None:
    normalized = _stringish(value)
    if normalized is None:
        return None
    try:
        return SignalSource[normalized.upper()]
    except KeyError:
        return None


def _normalized_type(value: object) -> SignalType | None:
    normalized = _stringish(value)
    if normalized is None:
        return None
    try:
        return SignalType[normalized.upper()]
    except KeyError:
        return None


def _stringish(value: object) -> str | None:
    return value.strip() if isinstance(value, str) and value.strip() else None
