"""Offline coverage for the daily, prior-only creator-history builder."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from scripts.build_creator_history import _snapshot_query, rows_from_cursor


def _write_births(path: Path) -> None:
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "mint": "first-win",
                    "creator_wallet": "creator-a",
                    "timestamp": 1_725_148_800_000,
                    "signature": "a",
                    "action": "create",
                },
                {
                    "mint": "first-rug",
                    "creator_wallet": "creator-a",
                    "timestamp": 1_725_148_801_000,
                    "signature": "b",
                    "action": "create",
                },
                {
                    "mint": "same-day",
                    "creator_wallet": "creator-a",
                    "timestamp": 1_725_235_200_000,
                    "signature": "c",
                    "action": "create",
                },
                {
                    "mint": "unknown-outcome",
                    "creator_wallet": "creator-b",
                    "timestamp": 1_725_148_802_000,
                    "signature": "d",
                    "action": "create",
                },
            ]
        ),
        path,
    )


def test_creator_history_is_prior_day_only_and_keeps_unknown_outcomes_out_of_rate(
    tmp_path: Path,
) -> None:
    births = tmp_path / "derived" / "births"
    births.mkdir(parents=True)
    _write_births(births / "2024-09-01.parquet")
    outcomes = tmp_path / "outcomes.csv"
    outcomes.write_text(
        "mint,birth_timestamp,reached_2x\n"
        "first-win,1725148800000,true\n"
        "first-rug,1725148801000,false\n"
        "same-day,1725235200000,false\n"
    )

    cursor, source_through = _snapshot_query(tmp_path, [outcomes], date(2024, 9, 2))
    try:
        rows = {row.creator_wallet: row for row in rows_from_cursor(cursor)}
    finally:
        cursor.close()

    assert source_through == date(2024, 9, 1)
    assert rows["creator-a"].prior_deploy_count == 2
    assert rows["creator-a"].prior_rug_observation_count == 2
    assert rows["creator-a"].prior_rug_count == 1
    assert rows["creator-a"].prior_rug_rate == 0.5
    assert rows["creator-b"].prior_deploy_count == 1
    assert rows["creator-b"].prior_rug_observation_count == 0
    assert rows["creator-b"].prior_rug_rate is None
