"""Optional disposable-PostgreSQL verification for the diagnostic ledger migration."""

from __future__ import annotations

import os
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
MIGRATION = PROJECT_ROOT / "migrations" / "0001_memecoin_decision_diagnostic.sql"
DISPOSABLE_DSN_ENV = "MEMECOIN_LEDGER_TEST_POSTGRES_DSN"


def test_migration_applies_to_explicit_disposable_postgres() -> None:
    """Apply and roll back only when a disposable test DSN is explicitly supplied."""

    dsn = os.getenv(DISPOSABLE_DSN_ENV)
    if not dsn:
        pytest.skip(f"Set {DISPOSABLE_DSN_ENV} to run against disposable PostgreSQL.")

    psycopg = pytest.importorskip("psycopg")
    connection = psycopg.connect(dsn)
    try:
        connection.execute(MIGRATION.read_text())
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
    finally:
        connection.rollback()
        connection.close()
