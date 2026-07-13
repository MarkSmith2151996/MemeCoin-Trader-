"""Coverage for SQLite-only live candidate observation persistence."""

from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

import pytest

from src.core.database import init_db, record_candidate_observation


def _record(db: Path, **kwargs):
    return asyncio.run(record_candidate_observation(db, **kwargs))


def test_first_observation_is_new(tmp_path: Path) -> None:
    db = tmp_path / "observations.db"
    asyncio.run(init_db(db))

    observation = _record(
        db,
        run_id="run-one",
        mint_address="mint-one",
        source_names=("pump_fun",),
        source_event_types=("new_pool",),
        candidate_mode="launch",
        strict_result="rejected",
        strict_labels=("top10_holder_check_failed",),
    )

    assert observation.is_new_mint is True
    assert observation.seen_count_total == 1
    assert observation.seen_count_run == 1
    assert observation.repeat_label == "new_mint"


def test_second_run_observation_is_repeat(tmp_path: Path) -> None:
    db = tmp_path / "repeat.db"
    asyncio.run(init_db(db))
    first = _record(db, run_id="run-one", mint_address="mint-repeat")
    second = _record(db, run_id="run-two", mint_address="mint-repeat")

    assert second.id != first.id
    assert second.is_new_mint is False
    assert second.first_seen_at == first.first_seen_at
    assert second.seen_count_total == 2
    assert second.repeat_label == "repeat_prior_run"


def test_same_run_sources_collapse_to_one_known_mint(tmp_path: Path) -> None:
    db = tmp_path / "sources.db"
    asyncio.run(init_db(db))
    first = _record(
        db,
        run_id="run-one",
        mint_address="mint-sources",
        source_names=("pump_fun",),
        source_event_types=("new_pool",),
    )
    repeated = _record(
        db,
        run_id="run-one",
        mint_address="mint-sources",
        source_names=("onchain",),
        source_event_types=("volume_spike",),
    )

    assert repeated.id == first.id
    assert repeated.seen_count_total == 2
    assert repeated.seen_count_run == 2
    assert repeated.repeat_label == "duplicate_same_run_collapsed"
    with sqlite3.connect(db) as connection:
        row_count = connection.execute("SELECT COUNT(*) FROM live_candidate_observations").fetchone()[0]
        sources = connection.execute(
            "SELECT source_names_json FROM live_candidate_observations"
        ).fetchone()[0]
    assert row_count == 1
    assert sources == '["onchain", "pump_fun"]'


def test_missing_mint_is_rejected(tmp_path: Path) -> None:
    db = tmp_path / "missing-mint.db"
    asyncio.run(init_db(db))

    with pytest.raises(ValueError, match="mint_address is required"):
        _record(db, run_id="run-one", mint_address="  ")


def test_observation_writes_do_not_create_trades_or_positions(tmp_path: Path) -> None:
    db = tmp_path / "no-trades.db"
    asyncio.run(init_db(db))
    _record(db, run_id="run-one", mint_address="mint-safe")

    with sqlite3.connect(db) as connection:
        trades = connection.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
        positions = connection.execute("SELECT COUNT(*) FROM positions").fetchone()[0]
    assert trades == 0
    assert positions == 0
