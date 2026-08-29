BEGIN;

-- Apply as the custodian schema owner, not as memecoin_writer.
ALTER TABLE memecoin.candidates
    ADD COLUMN IF NOT EXISTS creator_wallet TEXT,
    ADD COLUMN IF NOT EXISTS creator_initial_buy REAL,
    ADD COLUMN IF NOT EXISTS creator_initial_buy_sol REAL,
    ADD COLUMN IF NOT EXISTS creator_self_snipe_pct REAL,
    ADD COLUMN IF NOT EXISTS creator_prior_deploy_count INTEGER,
    ADD COLUMN IF NOT EXISTS creator_prior_rug_rate DOUBLE PRECISION;

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

GRANT SELECT ON memecoin.creator_history TO claude_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON memecoin.creator_history TO memecoin_writer;

COMMIT;
