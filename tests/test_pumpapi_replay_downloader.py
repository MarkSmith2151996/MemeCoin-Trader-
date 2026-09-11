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
