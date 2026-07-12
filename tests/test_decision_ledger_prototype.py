"""Static safety checks for the disabled decision-ledger prototype."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from src.core.decision_ledger import (
    LedgerDecisionLookup,
    LedgerDecisionSearch,
    LedgerDecisionEvidence,
    LedgerProviderObservation,
    LedgerPrototypeDisabledError,
    read_memecoin_decision,
    record_provider_observation,
    search_memecoin_decisions,
    write_memecoin_decision,
)
from src.core.models import CheckResult


PROJECT_ROOT = Path(__file__).resolve().parents[1]
MIGRATION = PROJECT_ROOT / "migrations" / "0001_memecoin_decision_diagnostic.sql"


def test_migration_is_limited_to_diagnostic_core_tables() -> None:
    sql = MIGRATION.read_text()

    for table in ("decisions", "risk_check_results", "rejection_records", "provider_snapshots"):
        assert f"memecoin_decision.{table}" in sql
    for forbidden in ("trade_entries", "trade_exits", "paper_positions", "attribution_links", "later_marks"):
        assert f"memecoin_decision.{forbidden}" not in sql
    assert "result IN ('PASS', 'FAIL', 'UNKNOWN')" in sql


def test_prototype_preserves_unknown_evidence_and_cannot_write() -> None:
    evidence = LedgerDecisionEvidence(
        decision_id="synthetic-decision",
        mint_address="synthetic-mint",
        checks={"liquidity_check": CheckResult.UNKNOWN},
    )

    assert evidence.checks["liquidity_check"] is CheckResult.UNKNOWN
    assert evidence.provider_status == "unknown"
    with pytest.raises(LedgerPrototypeDisabledError, match="disabled"):
        write_memecoin_decision(evidence)


def test_narrow_provider_and_read_contracts_remain_disabled() -> None:
    observation = LedgerProviderObservation(
        snapshot_id="synthetic-snapshot",
        mint_address="synthetic-mint",
        provider_name="synthetic-provider",
        provider_status="unavailable",
        observed_at="2026-07-12T00:00:00+00:00",
        unavailable_reason="not_collected",
    )
    lookup = LedgerDecisionLookup(decision_id="synthetic-decision")
    search = LedgerDecisionSearch(mint_address="synthetic-mint")

    for operation, request in (
        (record_provider_observation, observation),
        (read_memecoin_decision, lookup),
        (search_memecoin_decisions, search),
    ):
        with pytest.raises(LedgerPrototypeDisabledError, match="disabled"):
            operation(request)


def test_search_requires_a_narrow_filter_and_bounded_limit() -> None:
    with pytest.raises(ValueError, match="required"):
        LedgerDecisionSearch()
    with pytest.raises(ValueError):
        LedgerDecisionSearch(source="pump_fun", limit=101)


def test_boundary_has_no_sql_or_database_client() -> None:
    source = (PROJECT_ROOT / "src/core/decision_ledger.py").read_text()

    assert "aiosqlite" not in source
    assert "psycopg" not in source
    assert ".execute(" not in source
    assert "SELECT " not in source


def test_runtime_paths_do_not_import_or_call_ledger_boundary() -> None:
    excluded = {PROJECT_ROOT / "src/core/decision_ledger.py"}
    for path in (PROJECT_ROOT / "src").rglob("*.py"):
        if path in excluded:
            continue
        tree = ast.parse(path.read_text())
        imported = [
            node.module or ""
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
        ]
        assert "src.core.decision_ledger" not in imported, path
        assert "write_memecoin_decision" not in path.read_text(), path
