-- gate_value is JSONB so numeric thresholds and schedule arrays share one
-- strategy-as-data table. Numeric rows are decoded as floats by services.strategy.
INSERT INTO memecoin.gate_config (
    strategy, gate_name, gate_value, enabled, updated_at, updated_by, notes
) VALUES
    ('BT', 'mcap_floor', '5100'::jsonb, TRUE, NOW(), 'MT-664', 'Current Strategy B floor'),
    ('BT', 'mcap_ceiling', '50000'::jsonb, TRUE, NOW(), 'MT-671', 'Current Strategy B ceiling'),
    ('BT', 'min_age_seconds', '22'::jsonb, TRUE, NOW(), 'MT-664', 'Current Strategy B minimum evaluation age'),
    ('BT', 'max_age_seconds', '1320'::jsonb, TRUE, NOW(), 'MT-664', 'Current Strategy B maximum age'),
    ('BT', 'age_offset_seconds', '39'::jsonb, TRUE, NOW(), 'MT-671', 'Jupiter age correction measured by MT-617'),
    ('BT', 'txn_count_adjustment', '1.24'::jsonb, TRUE, NOW(), 'MT-671', 'Jupiter transaction-count correction'),
    ('BT', 'min_volume_usd', '500'::jsonb, TRUE, NOW(), 'MT-664', 'Current Strategy B volume floor'),
    ('BT', 'min_volume_to_mcap_ratio', '0.005'::jsonb, TRUE, NOW(), 'MT-671', 'Reject dead-volume observations'),
    ('BT', 'max_volume_to_mcap_ratio', '50'::jsonb, TRUE, NOW(), 'MT-671', 'Reject wash-volume observations'),
    ('BT', 'min_buy_sell_ratio', '0.5'::jsonb, TRUE, NOW(), 'MT-664', 'Current Strategy B buy/sell floor'),
    ('BT', 'min_pool_sol_bonding', '5'::jsonb, TRUE, NOW(), 'MT-664', 'Current bonding-curve pool floor'),
    ('BT', 'min_pool_sol_graduated', '5'::jsonb, TRUE, NOW(), 'MT-664', 'Current graduated-pool floor'),
    ('BT', 'creator_holdings_max', '0'::jsonb, TRUE, NOW(), 'MT-664', 'Creator holdings must be zero when known'),
    ('BT', 'max_top_holder_pct', '100'::jsonb, TRUE, NOW(), 'MT-671', 'Require known holder evidence at or below V1 ceiling'),
    ('BT', 'score_threshold_bonding', '40'::jsonb, TRUE, NOW(), 'MT-664', 'Current strength-score floor'),
    ('BT', 'score_threshold_graduated', '40'::jsonb, TRUE, NOW(), 'MT-671', 'Current graduated strength-score floor'),
    ('BT', 'blocked_weekdays', '[2]'::jsonb, TRUE, NOW(), 'MT-664', 'Python weekday numbering, Monday is zero'),
    ('BT', 'blocked_hours_utc', '[0, 19, 20, 21]'::jsonb, TRUE, NOW(), 'MT-664', 'UTC entry dead zones'),
    ('BT', 'max_open', '5'::jsonb, TRUE, NOW(), 'MT-664', 'Current Strategy B capacity')
ON CONFLICT (strategy, gate_name) DO NOTHING;
