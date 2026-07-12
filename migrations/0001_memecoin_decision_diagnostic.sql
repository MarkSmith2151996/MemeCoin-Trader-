-- Diagnostic-only PostgreSQL prototype. This migration has no runtime caller.
-- SQLite remains the execution, paper-state, and PnL source of truth.

CREATE SCHEMA IF NOT EXISTS memecoin_decision;

CREATE TABLE IF NOT EXISTS memecoin_decision.provider_snapshots (
    snapshot_id UUID PRIMARY KEY,
    mint_address TEXT NOT NULL,
    provider_name TEXT NOT NULL,
    provider_status TEXT NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    field_presence_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    normalized_data_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    unavailable_reason TEXT,
    source_decision_id UUID,
    content_hash TEXT
);

CREATE TABLE IF NOT EXISTS memecoin_decision.decisions (
    decision_id UUID PRIMARY KEY,
    mint_address TEXT NOT NULL,
    symbol TEXT,
    name TEXT,
    decision_type TEXT NOT NULL CHECK (decision_type IN ('seen', 'rejected', 'accepted', 'entry', 'exit', 'labeled')),
    decision_time TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL,
    mode TEXT NOT NULL CHECK (mode IN ('paper', 'live')),
    outcome_status TEXT NOT NULL,
    main_reason TEXT,
    failed_check TEXT,
    strategy_version TEXT,
    agent_version TEXT,
    parameters_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    risk_checks_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    provider_data_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    snapshot_id UUID REFERENCES memecoin_decision.provider_snapshots (snapshot_id),
    related_trade_id TEXT,
    related_candidate_id TEXT,
    supersedes_decision_id UUID REFERENCES memecoin_decision.decisions (decision_id)
);

CREATE TABLE IF NOT EXISTS memecoin_decision.risk_check_results (
    risk_check_result_id UUID PRIMARY KEY,
    decision_id UUID NOT NULL REFERENCES memecoin_decision.decisions (decision_id),
    check_name TEXT NOT NULL,
    result TEXT NOT NULL CHECK (result IN ('PASS', 'FAIL', 'UNKNOWN')),
    reason_code TEXT,
    observed_value_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    threshold_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    provider_snapshot_id UUID REFERENCES memecoin_decision.provider_snapshots (snapshot_id),
    checked_at TIMESTAMPTZ NOT NULL,
    UNIQUE (decision_id, check_name)
);

CREATE TABLE IF NOT EXISTS memecoin_decision.rejection_records (
    rejection_id UUID PRIMARY KEY,
    decision_id UUID NOT NULL UNIQUE REFERENCES memecoin_decision.decisions (decision_id),
    rejection_reason TEXT NOT NULL,
    failed_check TEXT NOT NULL,
    risk_profile TEXT NOT NULL,
    candidate_mode TEXT NOT NULL,
    attention_score NUMERIC,
    risk_score NUMERIC,
    snapshot_coverage_status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS decisions_mint_time_idx
    ON memecoin_decision.decisions (mint_address, decision_time DESC);
CREATE INDEX IF NOT EXISTS decisions_source_mode_time_idx
    ON memecoin_decision.decisions (source, mode, decision_time DESC);
CREATE INDEX IF NOT EXISTS decisions_related_trade_idx
    ON memecoin_decision.decisions (related_trade_id) WHERE related_trade_id IS NOT NULL;
