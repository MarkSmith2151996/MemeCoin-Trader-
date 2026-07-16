"""Focused tests for archived paper position exclusion (TD-084)."""

import asyncio
from datetime import UTC, datetime
from pathlib import Path

from src.cli import _count_effective_open_positions
from src.core.config import load_settings
from src.core.database import init_db, record_position
from src.core.models import Position
from src.strategy.position_manager import PositionManager


class TestPaperSoakArchiveExclusion:
    """TD-084: paper-soak capacity counting must exclude archived positions."""

    async def _seed_positions(self, db_path: Path) -> None:
        """Create 3 paper positions (1 active, 2 archived) in the DB."""
        await init_db(db_path)
        unarchived = Position(
            mint_address="active-mint-1",
            entry_trade_id="trade-1",
            amount_sol=1.0,
            token_amount=1000.0,
            entry_price_sol=0.001,
            mode="paper",
        )
        archived_1 = Position(
            mint_address="archived-mint-1",
            entry_trade_id="trade-2",
            amount_sol=1.0,
            token_amount=1000.0,
            entry_price_sol=0.001,
            mode="paper",
            archived=True,
            archived_at=datetime.now(UTC),
            archive_reason="legacy_fill_quality",
        )
        archived_2 = Position(
            mint_address="archived-mint-2",
            entry_trade_id="trade-3",
            amount_sol=1.0,
            token_amount=1000.0,
            entry_price_sol=0.001,
            mode="paper",
            archived=True,
            archived_at=datetime.now(UTC),
            archive_reason="legacy_fill_quality",
        )
        for pos in (unarchived, archived_1, archived_2):
            await record_position(db_path, pos)

    def test_count_effective_open_positions_excludes_archived(self, tmp_path: Path) -> None:
        async def run() -> None:
            db_path = tmp_path / "test-archive-count.db"
            await self._seed_positions(db_path)
            count = await _count_effective_open_positions(db_path)
            assert count == 1, f"Expected 1 open position, got {count}"

        asyncio.run(run())

    def test_get_all_open_excludes_archived_with_persisted_positions(self, tmp_path: Path) -> None:
        async def run() -> None:
            db_path = tmp_path / "test-get-all-open.db"
            await self._seed_positions(db_path)
            settings = load_settings()
            manager = PositionManager(db_path, settings, use_persisted_positions=True)
            first_call = await manager.get_all_open(mode="paper")
            assert len(first_call) == 1, f"Expected 1 position on first call, got {len(first_call)}"
            second_call = await manager.get_all_open(mode="paper")
            assert len(second_call) == 1, f"Expected 1 position on second call (cache warm), got {len(second_call)}"

        asyncio.run(run())

    def test_position_json_roundtrip_preserves_archived(self) -> None:
        original = Position(
            mint_address="roundtrip-mint",
            entry_trade_id="trade-rt",
            amount_sol=1.0,
            token_amount=1000.0,
            entry_price_sol=0.001,
            mode="paper",
            archived=True,
            archived_at=datetime.now(UTC),
            archive_reason="legacy_fill_quality",
        )
        raw_json = original.model_dump_json()
        restored = Position.model_validate_json(raw_json)
        assert restored.archived is True
        assert restored.archive_reason == "legacy_fill_quality"
        assert restored.archived_at is not None
