BEGIN;

CREATE SCHEMA IF NOT EXISTS memecoin AUTHORIZATION custodian;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'memecoin_writer') THEN
        CREATE ROLE memecoin_writer NOLOGIN;
    END IF;
END
$$;

CREATE TABLE IF NOT EXISTS memecoin.candidates (
    id SERIAL PRIMARY KEY,
    mint_address TEXT NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    source TEXT,
    age_seconds REAL,
    corrected_age_seconds REAL,
    mcap_usd REAL,
    volume_usd REAL,
    buy_volume_usd REAL,
    sell_volume_usd REAL,
    txn_buys INTEGER,
    txn_sells INTEGER,
    buy_sell_ratio REAL,
    liquidity_usd REAL,
    fdv_usd REAL,
    price_sol REAL,
    price_usd REAL,
    pool_sol REAL,
    pool_type TEXT,
    creator_holdings_pct REAL,
    mint_authority_revoked BOOLEAN,
    freeze_authority_revoked BOOLEAN,
    top_holder_pct REAL,
    security_source TEXT,
    security_checked_at TIMESTAMPTZ,
    unique_wallets INTEGER,
    price_change_5m REAL,
    price_change_1h REAL,
    creator_wallet TEXT,
    creator_initial_buy REAL,
    creator_initial_buy_sol REAL,
    creator_self_snipe_pct REAL,
    creator_prior_deploy_count INTEGER,
    creator_prior_rug_rate DOUBLE PRECISION,
    strength_score REAL,
    raw_json JSONB
);

ALTER TABLE memecoin.candidates
    ADD COLUMN IF NOT EXISTS corrected_age_seconds REAL,
    ADD COLUMN IF NOT EXISTS buy_volume_usd REAL,
    ADD COLUMN IF NOT EXISTS sell_volume_usd REAL,
    ADD COLUMN IF NOT EXISTS mint_authority_revoked BOOLEAN,
    ADD COLUMN IF NOT EXISTS freeze_authority_revoked BOOLEAN,
    ADD COLUMN IF NOT EXISTS top_holder_pct REAL,
    ADD COLUMN IF NOT EXISTS security_source TEXT,
    ADD COLUMN IF NOT EXISTS security_checked_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS creator_wallet TEXT,
    ADD COLUMN IF NOT EXISTS creator_initial_buy REAL,
    ADD COLUMN IF NOT EXISTS creator_initial_buy_sol REAL,
    ADD COLUMN IF NOT EXISTS creator_self_snipe_pct REAL,
    ADD COLUMN IF NOT EXISTS creator_prior_deploy_count INTEGER,
    ADD COLUMN IF NOT EXISTS creator_prior_rug_rate DOUBLE PRECISION;

CREATE INDEX IF NOT EXISTS candidates_observed_mint_idx
    ON memecoin.candidates (observed_at, mint_address);

CREATE INDEX IF NOT EXISTS candidates_mint_observed_idx
    ON memecoin.candidates (mint_address, observed_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS candidates_creator_observed_idx
    ON memecoin.candidates (creator_wallet, observed_at DESC)
    WHERE creator_wallet IS NOT NULL;

CREATE TABLE IF NOT EXISTS memecoin.creator_history (
    creator_wallet TEXT PRIMARY KEY,
    as_of_date DATE NOT NULL,
    source_through_date DATE,
    prior_deploy_count INTEGER NOT NULL,
    prior_rug_observation_count INTEGER NOT NULL,
    prior_rug_count INTEGER NOT NULL,
    prior_rug_rate DOUBLE PRECISION,
    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS memecoin.positions (
    id UUID PRIMARY KEY,
    mint_address TEXT NOT NULL,
    entry_price_sol REAL NOT NULL,
    amount_sol REAL NOT NULL,
    token_amount REAL,
    peak_price_sol REAL,
    trailing_armed BOOLEAN NOT NULL DEFAULT FALSE,
    status TEXT NOT NULL,
    mode TEXT NOT NULL,
    strategy TEXT,
    opened_at TIMESTAMPTZ NOT NULL,
    closed_at TIMESTAMPTZ,
    close_reason TEXT,
    close_price_sol REAL,
    realized_pnl_sol DOUBLE PRECISION,
    adjusted_pnl_sol DOUBLE PRECISION,
    candidate_id INTEGER REFERENCES memecoin.candidates(id),
    fill_quality TEXT,
    tx_signature TEXT,
    quarantined_at TIMESTAMPTZ,
    quarantine_reason TEXT
);

ALTER TABLE memecoin.positions
    ADD COLUMN IF NOT EXISTS quarantined_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS quarantine_reason TEXT;

DROP INDEX IF EXISTS memecoin.positions_one_open_mint_mode_idx;
CREATE UNIQUE INDEX IF NOT EXISTS positions_one_open_mint_mode_idx
    ON memecoin.positions (mint_address, mode)
    WHERE status IN ('open', 'quarantined');

CREATE INDEX IF NOT EXISTS positions_open_strategy_idx
    ON memecoin.positions (strategy, mode, opened_at)
    WHERE status = 'open';

CREATE INDEX IF NOT EXISTS positions_recent_hard_stop_idx
    ON memecoin.positions (mint_address, closed_at DESC)
    WHERE status = 'closed' AND close_reason = 'hard_stop';

ALTER TABLE memecoin.positions
    ALTER COLUMN realized_pnl_sol TYPE DOUBLE PRECISION,
    ALTER COLUMN adjusted_pnl_sol TYPE DOUBLE PRECISION;

CREATE TABLE IF NOT EXISTS memecoin.trades (
    id UUID PRIMARY KEY,
    position_id UUID REFERENCES memecoin.positions(id),
    mint_address TEXT NOT NULL,
    side TEXT NOT NULL,
    amount_sol REAL,
    token_amount REAL,
    price_sol REAL,
    slippage_bps INTEGER,
    tx_signature TEXT,
    mode TEXT,
    executed_at TIMESTAMPTZ NOT NULL,
    metadata JSONB
);

CREATE INDEX IF NOT EXISTS trades_position_executed_idx
    ON memecoin.trades (position_id, executed_at);

CREATE TABLE IF NOT EXISTS memecoin.position_mark_evaluations (
    id BIGSERIAL PRIMARY KEY,
    position_id UUID NOT NULL REFERENCES memecoin.positions(id),
    mint_address TEXT NOT NULL,
    evaluated_at TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL,
    trigger_price_sol DOUBLE PRECISION,
    usable BOOLEAN NOT NULL,
    diagnostic TEXT NOT NULL,
    exit_reason TEXT
);

CREATE INDEX IF NOT EXISTS position_mark_evaluations_position_idx
    ON memecoin.position_mark_evaluations (position_id, evaluated_at DESC);

CREATE TABLE IF NOT EXISTS memecoin.gate_config (
    id SERIAL PRIMARY KEY,
    strategy TEXT NOT NULL,
    gate_name TEXT NOT NULL,
    gate_value JSONB NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by TEXT,
    notes TEXT,
    UNIQUE(strategy, gate_name)
);

CREATE TABLE IF NOT EXISTS memecoin.exit_config (
    id SERIAL PRIMARY KEY,
    strategy TEXT NOT NULL,
    param_name TEXT NOT NULL,
    param_value REAL NOT NULL,
    UNIQUE(strategy, param_name)
);

CREATE TABLE IF NOT EXISTS memecoin.runtime_events (
    id UUID PRIMARY KEY,
    occurred_at TIMESTAMPTZ NOT NULL,
    event_type TEXT NOT NULL,
    reason TEXT,
    details JSONB
);

CREATE INDEX IF NOT EXISTS runtime_events_occurred_idx
    ON memecoin.runtime_events (occurred_at DESC);

CREATE TABLE IF NOT EXISTS memecoin.daily_stats (
    date DATE PRIMARY KEY,
    strategy TEXT,
    trades INTEGER NOT NULL,
    wins INTEGER NOT NULL,
    pnl_sol REAL NOT NULL,
    win_rate REAL NOT NULL
);

CREATE OR REPLACE FUNCTION memecoin.refresh_daily_stats(p_strategy TEXT DEFAULT 'BT')
RETURNS VOID
LANGUAGE SQL
AS $$
    INSERT INTO memecoin.daily_stats (date, strategy, trades, wins, pnl_sol, win_rate)
    SELECT
        (closed_at AT TIME ZONE 'UTC')::date,
        p_strategy,
        COUNT(*)::integer,
        COUNT(*) FILTER (WHERE realized_pnl_sol > 0)::integer,
        COALESCE(SUM(realized_pnl_sol), 0)::real,
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
        win_rate = EXCLUDED.win_rate;
$$;

GRANT USAGE ON SCHEMA memecoin TO claude_reader, memecoin_writer;
GRANT SELECT ON ALL TABLES IN SCHEMA memecoin TO claude_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA memecoin TO memecoin_writer;
GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA memecoin TO memecoin_writer;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA memecoin TO memecoin_writer;
GRANT EXECUTE ON FUNCTION memecoin.refresh_daily_stats(TEXT) TO memecoin_writer;
GRANT memecoin_writer TO custodian;

ALTER DEFAULT PRIVILEGES IN SCHEMA memecoin
    GRANT SELECT ON TABLES TO claude_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA memecoin
    GRANT SELECT ON TABLES TO memecoin_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA memecoin
    GRANT INSERT, UPDATE, DELETE ON TABLES TO memecoin_writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA memecoin
    GRANT USAGE, SELECT ON SEQUENCES TO memecoin_writer;

COMMIT;
