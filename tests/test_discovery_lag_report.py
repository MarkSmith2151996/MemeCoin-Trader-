"""Offline coverage for MT-563 discovery-lag telemetry helpers."""

from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

from scripts import discovery_lag_report
from scripts.run_strategy_b import classify_token_source
from src.core.database import init_db, record_discovery_lag


def test_source_classifier_uses_mint_and_pool_metadata() -> None:
    assert classify_token_source({"id": "mintpump", "firstPool": {"id": "mintpump"}}) == "pump"
    assert classify_token_source({"id": "mintpump", "firstPool": {"id": "raydiumPool"}}) == "raydium"
    assert classify_token_source({"id": "mint", "firstPool": {"id": "poolpump"}}) == "pumpswap"
    assert classify_token_source({"id": "mint", "firstPool": {"id": "meteoraPool"}}) == "unknown"


def test_report_handles_pre_schema_database(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "trades.db"
    sqlite3.connect(db_path).close()
    monkeypatch.setattr(discovery_lag_report, "DB_PATH", db_path)

    assert discovery_lag_report.load_samples() == []


def test_report_reads_persisted_gate_outcomes(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "trades.db"
    asyncio.run(init_db(db_path))
    asyncio.run(record_discovery_lag(
        db_path,
        mint_address="pass-mint",
        token_source="pump",
        created_at="2026-08-15T12:00:00+00:00",
        detected_at="2026-08-15T12:00:04+00:00",
        lag_seconds=4.0,
        passed_gates=True,
    ))
    asyncio.run(record_discovery_lag(
        db_path,
        mint_address="fail-mint",
        token_source="raydium",
        created_at="2026-08-15T12:00:00+00:00",
        detected_at="2026-08-15T12:00:12+00:00",
        lag_seconds=12.0,
        passed_gates=False,
    ))
    monkeypatch.setattr(discovery_lag_report, "DB_PATH", db_path)

    report = discovery_lag_report.render(discovery_lag_report.load_samples())

    assert "| pump | 1 | 4.0s | 4.0s | 4.0s |" in report
    assert "Passed gates (1)" in report
    assert "Rejected    (1)" in report
    assert "Insufficient samples for a definitive verdict: 2 < 500." in report
