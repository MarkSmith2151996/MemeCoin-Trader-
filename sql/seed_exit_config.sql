INSERT INTO memecoin.exit_config (strategy, param_name, param_value) VALUES
    ('BT', 'trailing_stop_pct', 2),
    ('BT', 'trailing_arm_pct', 2),
    ('BT', 'hard_stop_pct', 8),
    ('BT', 'take_profit_pct', 150),
    ('BT', 'time_stop_minutes', 10)
ON CONFLICT (strategy, param_name) DO NOTHING;
