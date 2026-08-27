-- Historical SQLite rows are streamed by scripts/migrate_to_postgres.py.
-- PostgreSQL cannot directly read SQLite without a foreign-data wrapper, so the
-- Python migration owns type normalization, UUID validation, and batching.
-- Run this reconciliation after a successful import.

SELECT memecoin.refresh_daily_stats('BT');

SELECT
    (SELECT COUNT(*) FROM memecoin.candidates) AS candidates,
    (SELECT COUNT(*) FROM memecoin.positions) AS positions,
    (SELECT COUNT(*) FROM memecoin.trades) AS trades;
