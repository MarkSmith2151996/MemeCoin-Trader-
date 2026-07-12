"""Optional disposable-PostgreSQL verification for the diagnostic ledger migration."""

from __future__ import annotations

import os
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
MIGRATION = PROJECT_ROOT / "migrations" / "0001_memecoin_decision_diagnostic.sql"
DISPOSABLE_DSN_ENV = "MEMECOIN_LEDGER_TEST_POSTGRES_DSN"


def test_migration_applies_to_explicit_disposable_postgres() -> None:
    """Verify migration constraints and rollback only in an empty disposable database."""

    dsn = os.getenv(DISPOSABLE_DSN_ENV)
    if not dsn:
        pytest.skip(f"Set {DISPOSABLE_DSN_ENV} to run against disposable PostgreSQL.")

    psycopg = pytest.importorskip("psycopg")
    connection = psycopg.connect(dsn)
    migration_applied = False
    try:
        existing_schema = connection.execute(
            "SELECT to_regnamespace('memecoin_decision')"
        ).fetchone()[0]
        if existing_schema is not None:
            pytest.skip("Disposable PostgreSQL fixture must not already contain memecoin_decision.")

        connection.execute(MIGRATION.read_text())
        migration_applied = True
        rows = connection.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'memecoin_decision'
            ORDER BY table_name
            """
        ).fetchall()
        assert {row[0] for row in rows} == {
            "decisions",
            "provider_snapshots",
            "rejection_records",
            "risk_check_results",
        }

        primary_keys = connection.execute(
            """
            SELECT table_name
            FROM information_schema.table_constraints
            WHERE table_schema = 'memecoin_decision'
              AND constraint_type = 'PRIMARY KEY'
            """
        ).fetchall()
        assert {row[0] for row in primary_keys} == {
            "decisions",
            "provider_snapshots",
            "rejection_records",
            "risk_check_results",
        }

        required_columns = connection.execute(
            """
            SELECT table_name, column_name
            FROM information_schema.columns
            WHERE table_schema = 'memecoin_decision'
              AND is_nullable = 'NO'
            """
        ).fetchall()
        assert {
            ("decisions", "mint_address"),
            ("decisions", "decision_type"),
            ("decisions", "mode"),
            ("provider_snapshots", "provider_name"),
            ("risk_check_results", "decision_id"),
            ("risk_check_results", "result"),
            ("rejection_records", "decision_id"),
        }.issubset(set(required_columns))

        foreign_key_targets = connection.execute(
            """
            SELECT conrelid::regclass::text, confrelid::regclass::text
            FROM pg_constraint
            WHERE contype = 'f'
              AND connamespace = 'memecoin_decision'::regnamespace
            """
        ).fetchall()
        assert {
            ("memecoin_decision.decisions", "memecoin_decision.provider_snapshots"),
            ("memecoin_decision.risk_check_results", "memecoin_decision.decisions"),
            ("memecoin_decision.risk_check_results", "memecoin_decision.provider_snapshots"),
            ("memecoin_decision.rejection_records", "memecoin_decision.decisions"),
        }.issubset(set(foreign_key_targets))
    finally:
        connection.rollback()
        if migration_applied:
            assert connection.execute("SELECT to_regnamespace('memecoin_decision')").fetchone()[0] is None
        connection.close()
