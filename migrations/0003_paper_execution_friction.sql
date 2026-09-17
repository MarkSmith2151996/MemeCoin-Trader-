BEGIN;

ALTER TABLE memecoin.positions
    ADD COLUMN IF NOT EXISTS entry_pool_sol REAL,
    ADD COLUMN IF NOT EXISTS quoted_token_amount REAL,
    ADD COLUMN IF NOT EXISTS slippage_entry_pct REAL,
    ADD COLUMN IF NOT EXISTS slippage_exit_pct REAL,
    ADD COLUMN IF NOT EXISTS dex_fee_entry_sol REAL,
    ADD COLUMN IF NOT EXISTS dex_fee_exit_sol REAL,
    ADD COLUMN IF NOT EXISTS priority_fee_total_sol REAL,
    ADD COLUMN IF NOT EXISTS entry_attempts INTEGER,
    ADD COLUMN IF NOT EXISTS exit_delay_seconds REAL,
    ADD COLUMN IF NOT EXISTS gross_pnl_sol DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS net_pnl_sol DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS friction_total_sol DOUBLE PRECISION;

ALTER TABLE memecoin.daily_stats
    ADD COLUMN IF NOT EXISTS gross_pnl_sol DOUBLE PRECISION NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS net_pnl_sol DOUBLE PRECISION NOT NULL DEFAULT 0;

CREATE OR REPLACE FUNCTION memecoin.refresh_daily_stats(p_strategy TEXT DEFAULT 'BT')
RETURNS VOID
LANGUAGE SQL
AS $$
    INSERT INTO memecoin.daily_stats (
        date, strategy, trades, wins, pnl_sol, gross_pnl_sol, net_pnl_sol, win_rate
    )
    SELECT
        (closed_at AT TIME ZONE 'UTC')::date,
        p_strategy,
        COUNT(*)::integer,
        COUNT(*) FILTER (WHERE realized_pnl_sol > 0)::integer,
        COALESCE(SUM(realized_pnl_sol), 0)::real,
        COALESCE(SUM(COALESCE(gross_pnl_sol, realized_pnl_sol)), 0)::double precision,
        COALESCE(SUM(COALESCE(net_pnl_sol, realized_pnl_sol)), 0)::double precision,
        COALESCE(
            COUNT(*) FILTER (WHERE realized_pnl_sol > 0)::real / NULLIF(COUNT(*), 0),
            0
        )
    FROM memecoin.positions
    WHERE status = 'closed'
      AND strategy = p_strategy
      AND closed_at IS NOT NULL
    GROUP BY (closed_at AT TIME ZONE 'UTC')::date
    ON CONFLICT (date) DO UPDATE SET
        strategy = EXCLUDED.strategy,
        trades = EXCLUDED.trades,
        wins = EXCLUDED.wins,
        pnl_sol = EXCLUDED.pnl_sol,
        gross_pnl_sol = EXCLUDED.gross_pnl_sol,
        net_pnl_sol = EXCLUDED.net_pnl_sol,
        win_rate = EXCLUDED.win_rate;
$$;

COMMIT;
