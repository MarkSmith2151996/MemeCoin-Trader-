"""Contract tests for the resumable PumpApi archive downloader."""

from __future__ import annotations

import asyncio
import sys
from datetime import UTC, datetime
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import pumpapi_replay_downloader as downloader  # noqa: E402


class FakeResponse:
    status = 200
    headers: dict[str, str] = {}

    async def __aenter__(self) -> FakeResponse:
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None


class FakeSession:
    def head(self, *_args: object, **_kwargs: object) -> FakeResponse:
        return FakeResponse()


def test_head_entry_accepts_a_200_without_content_length() -> None:
    hour = downloader.ArchiveHour(datetime(2026, 4, 18, tzinfo=UTC))

    entry = asyncio.run(
        downloader.head_entry(FakeSession(), hour, asyncio.Semaphore(1))
    )

    assert entry == {
        "key": "2026/04/18/00",
        "url": "https://replay.pumpapi.io/2026/04/18/00.jsonl.zst",
        "status": "available",
    }


def test_iter_hours_honors_an_explicit_closed_date_range() -> None:
    hours = downloader.iter_hours(
        datetime(2026, 8, 22, tzinfo=UTC),
        datetime(2026, 8, 23, 23, tzinfo=UTC),
    )

    assert len(hours) == 48
    assert hours[0].key == "2026/08/22/00"
    assert hours[-1].key == "2026/08/23/23"


def test_completed_on_disk_requires_validated_matching_size(tmp_path: Path) -> None:
    root = tmp_path
    entry = {"key": "2026/08/22/00"}
    destination = downloader.file_path(root, entry)
    destination.parent.mkdir(parents=True)
    destination.write_bytes(b"1234")
    state = {"completed": {entry["key"]: {"size": 4, "validated": True}}, "failed": {}}

    assert downloader.completed_on_disk(root, entry, state)

    state["completed"][entry["key"]]["size"] = 3
    assert not downloader.completed_on_disk(root, entry, state)


def test_head_error_remains_a_download_candidate() -> None:
    manifest = {
        "entries": [
            {"key": "2026/08/22/00", "status": "available"},
            {"key": "2026/08/22/01", "status": "error"},
            {"key": "2026/08/22/02", "status": "missing"},
        ]
    }

    assert [entry["key"] for entry in downloader.downloadable_entries(manifest)] == [
        "2026/08/22/00",
        "2026/08/22/01",
    ]
