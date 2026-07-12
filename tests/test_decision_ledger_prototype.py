"""Static safety checks for the disabled decision-ledger prototype."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from src.core.decision_ledger import (
    LedgerDecisionLookup,
    LedgerDecisionSearch,
    LedgerDecisionEvidence,
    LedgerHistoricalImport,
    LedgerHistoricalProvenance,
    LedgerProviderObservation,
    LedgerPrototypeDisabledError,
    import_historical_ledger_evidence,
    read_memecoin_decision,
    record_provider_observation,
    search_memecoin_decisions,
    write_memecoin_decision,
)
from src.core.models import CheckResult
from tests.ledger_harness import SyntheticLedgerAdapter


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


def test_historical_import_requires_explicit_provenance_and_stays_disabled() -> None:
    decision = LedgerDecisionEvidence(
        decision_id="historical-decision",
        mint_address="historical-mint",
        checks={"liquidity_check": CheckResult.UNKNOWN},
    )
    import_request = LedgerHistoricalImport(
        import_id="historical-import",
        provenance=LedgerHistoricalProvenance(
            source_system="sqlite",
            source_table="paper_decisions",
            source_record_id="paper-decision-1",
            source_observed_at="2026-07-12T00:00:00+00:00",
            extraction_method="explicit_export",
        ),
        decision=decision,
        provider_observations=[
            LedgerProviderObservation(
                snapshot_id="historical-snapshot",
                mint_address="historical-mint",
                provider_name="historical-provider",
                provider_status="unknown",
                observed_at="2026-07-12T00:00:00+00:00",
                source_decision_id="historical-decision",
            )
        ],
    )

    assert import_request.outcome_status == "unknown"
    assert import_request.outcome_claim == "not_claimed"
    assert import_request.decision.checks["liquidity_check"] is CheckResult.UNKNOWN
    with pytest.raises(LedgerPrototypeDisabledError, match="disabled"):
        import_historical_ledger_evidence(import_request)


def test_historical_import_rejects_outcome_claims_and_unlinked_provider_evidence() -> None:
    decision = LedgerDecisionEvidence(decision_id="historical-decision", mint_address="historical-mint")
    provenance = LedgerHistoricalProvenance(
        source_system="sqlite",
        source_table="paper_decisions",
        source_record_id="paper-decision-1",
        source_observed_at="2026-07-12T00:00:00+00:00",
        extraction_method="explicit_export",
    )

    with pytest.raises(ValueError):
        LedgerHistoricalImport(
            import_id="historical-import",
            provenance=provenance,
            decision=decision,
            outcome_status="measurable",
        )
    with pytest.raises(ValueError, match="import-safe"):
        LedgerHistoricalImport(
            import_id="historical-import",
            provenance=provenance,
            decision=LedgerDecisionEvidence(
                decision_id="historical-decision",
                mint_address="historical-mint",
                outcome_status="measurable",
            ),
        )
    with pytest.raises(ValueError, match="reference"):
        LedgerHistoricalImport(
            import_id="historical-import",
            provenance=provenance,
            decision=decision,
            provider_observations=[
                LedgerProviderObservation(
                    snapshot_id="historical-snapshot",
                    mint_address="historical-mint",
                    provider_name="historical-provider",
                    provider_status="unknown",
                    observed_at="2026-07-12T00:00:00+00:00",
                )
            ],
        )


def test_search_requires_a_narrow_filter_and_bounded_limit() -> None:
    with pytest.raises(ValueError, match="required"):
        LedgerDecisionSearch()
    with pytest.raises(ValueError):
        LedgerDecisionSearch(source="pump_fun", limit=101)


def test_synthetic_harness_is_process_local_and_applies_contract_filters() -> None:
    adapter = SyntheticLedgerAdapter()
    accepted = LedgerDecisionEvidence(
        decision_id="accepted-decision",
        mint_address="mint-a",
        source="pump_fun",
        outcome_status="accepted",
    )
    rejected = LedgerDecisionEvidence(
        decision_id="rejected-decision",
        mint_address="mint-b",
        source="whale_tracker",
        outcome_status="rejected",
    )
    adapter.record_decision(accepted)
    adapter.record_decision(rejected)

    assert adapter.read_decision(LedgerDecisionLookup(decision_id="accepted-decision")) == accepted
    assert adapter.search_decisions(LedgerDecisionSearch(source="pump_fun")) == [accepted]
    assert SyntheticLedgerAdapter().read_decision(LedgerDecisionLookup(decision_id="accepted-decision")) is None


def test_synthetic_harness_rejects_duplicate_ids_and_has_no_sql_surface() -> None:
    adapter = SyntheticLedgerAdapter()
    evidence = LedgerDecisionEvidence(decision_id="decision", mint_address="mint")
    observation = LedgerProviderObservation(
        snapshot_id="snapshot",
        mint_address="mint",
        provider_name="provider",
        provider_status="unavailable",
        observed_at="2026-07-12T00:00:00+00:00",
    )
    adapter.record_decision(evidence)
    adapter.record_provider_observation(observation)

    with pytest.raises(ValueError, match="unique"):
        adapter.record_decision(evidence)
    with pytest.raises(ValueError, match="unique"):
        adapter.record_provider_observation(observation)

    source = (PROJECT_ROOT / "tests/ledger_harness.py").read_text()
    assert "sqlite" not in source
    assert "psycopg" not in source
    assert ".execute(" not in source


def test_boundary_has_no_sql_or_database_client() -> None:
    source = (PROJECT_ROOT / "src/core/decision_ledger.py").read_text()

    assert "aiosqlite" not in source
    assert "psycopg" not in source
    assert ".execute(" not in source
    assert "SELECT " not in source


def test_runtime_paths_do_not_import_or_call_diagnostic_boundaries() -> None:
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
        assert "ledger_harness" not in path.read_text(), path
        runtime_calls = [
            node.func.id
            for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
        ]
        assert "paper_rejected_provider_mark_coverage" not in runtime_calls, path
