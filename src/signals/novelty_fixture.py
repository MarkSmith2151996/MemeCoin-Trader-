"""Read caller-supplied JSON signal fixtures for novelty diagnostics."""

from __future__ import annotations

import json
from pathlib import Path

from src.core.models import Signal
from src.signals.novelty import NoveltySummary, summarize_novelty


def summarize_json_signal_fixture(path: str | Path) -> NoveltySummary:
    """Read one explicit JSON signal fixture and return its novelty summary.

    The fixture must be a top-level JSON array of normalized Signal records. This
    helper has no default path and never acquires, persists, or otherwise acts on
    the records beyond model validation and in-memory summarization.
    """

    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError("Signal fixture must be a top-level JSON array.")

    signals: list[Signal] = []
    for index, record in enumerate(payload):
        if not isinstance(record, dict):
            raise ValueError(f"Signal fixture record at index {index} must be an object.")
        signals.append(Signal.model_validate(record))
    return summarize_novelty(signals)
