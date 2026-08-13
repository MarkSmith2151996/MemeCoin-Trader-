-- MT-533: Pump.fun graduation backtest exports for Dune Analytics.
--
-- Both queries use Dune's decoded Pump.fun withdrawal instruction as the
-- graduation event, then require a wSOL pair trade on Raydium or PumpSwap.
-- The supplied market cap is an estimate: first observed post-graduation USD
-- price multiplied by Pump.fun's conventional 1B token supply.
--
-- Dune table names can vary by decoded-program release. If the SQL editor
-- reports a missing Pump.fun table, use Dune's data explorer to replace only
-- pumpdotfun_solana.pump_call_create / pump_call_withdraw with the current
-- decoded create / withdraw tables, retaining the selected aliases below.


-- ============================================================================
-- QUERY A: graduated_tokens.csv
-- Last 30 days of Pump.fun withdrawals with a Raydium/PumpSwap wSOL market.
-- Output: mint, graduation and launch times, estimated graduation market cap,
--         first-30-minute USD volume, buys, sells, and observed liquidity.
-- ============================================================================
WITH
launches AS (
    SELECT
        account_mint AS mint_address,
        MIN(call_block_time) AS launch_timestamp
    FROM pumpdotfun_solana.pump_call_create
    GROUP BY 1
),
graduations AS (
    SELECT
        account_mint AS mint_address,
        MIN(call_block_time) AS graduation_timestamp
    FROM pumpdotfun_solana.pump_call_withdraw
    WHERE call_block_time >= NOW() - INTERVAL '30' DAY
    GROUP BY 1
),
wsol_swaps AS (
    SELECT
        g.mint_address,
        g.graduation_timestamp,
        l.launch_timestamp,
        t.block_time,
        LOWER(t.project) AS dex,
        t.amount_usd,
        CASE
            WHEN t.token_bought_mint_address = g.mint_address THEN 'buy'
            ELSE 'sell'
        END AS side,
        CASE
            WHEN t.token_bought_mint_address = g.mint_address
                THEN t.token_bought_amount
            ELSE t.token_sold_amount
        END AS token_amount,
        CASE
            WHEN t.token_bought_mint_address = g.mint_address
                THEN t.amount_usd / NULLIF(t.token_bought_amount, 0)
            ELSE t.amount_usd / NULLIF(t.token_sold_amount, 0)
        END AS price_usd
    FROM graduations g
    LEFT JOIN launches l ON l.mint_address = g.mint_address
    JOIN dex_solana.trades t
        ON t.block_time >= g.graduation_timestamp
        AND t.block_time < g.graduation_timestamp + INTERVAL '30' MINUTE
        AND (
            (t.token_bought_mint_address = g.mint_address
             AND t.token_sold_mint_address = 'So11111111111111111111111111111111111111112')
            OR
            (t.token_sold_mint_address = g.mint_address
             AND t.token_bought_mint_address = 'So11111111111111111111111111111111111111112')
        )
    WHERE LOWER(t.project) IN ('raydium', 'pumpswap', 'pump_swap')
),
first_swap AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY mint_address ORDER BY block_time) AS row_num
        FROM wsol_swaps
    )
    WHERE row_num = 1
)
SELECT
    s.mint_address,
    s.graduation_timestamp,
    s.launch_timestamp,
    DATE_DIFF('second', s.launch_timestamp, s.graduation_timestamp) / 60.0
        AS age_minutes_at_graduation,
    s.dex AS graduation_dex,
    -- Pump.fun tokens conventionally have a 1,000,000,000 token supply.
    s.price_usd * 1000000000.0 AS market_cap_usd_at_graduation,
    SUM(s.amount_usd) AS volume_usd_first_30m,
    COUNT_IF(s.side = 'buy') AS buy_count_first_30m,
    COUNT_IF(s.side = 'sell') AS sell_count_first_30m,
    -- DEX trade data has no pool-reserve snapshot; this is the first observed
    -- USD trade notional, retained as an explicit liquidity proxy.
    MAX(f.amount_usd) AS liquidity_added_usd_proxy
FROM first_swap f
JOIN wsol_swaps s ON s.mint_address = f.mint_address
GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY graduation_timestamp DESC;


-- ============================================================================
-- QUERY B: graduated_token_swaps.csv
-- First two hours of wSOL swaps after every Query A graduation event.
-- Output columns intentionally match scripts/dune_backtest.py.
-- ============================================================================
WITH graduations AS (
    SELECT
        account_mint AS mint_address,
        MIN(call_block_time) AS graduation_timestamp
    FROM pumpdotfun_solana.pump_call_withdraw
    WHERE call_block_time >= NOW() - INTERVAL '30' DAY
    GROUP BY 1
),
graduated_mints AS (
    SELECT DISTINCT g.mint_address, g.graduation_timestamp
    FROM graduations g
    JOIN dex_solana.trades t
        ON t.block_time >= g.graduation_timestamp
        AND t.block_time < g.graduation_timestamp + INTERVAL '2' HOUR
        AND (
            (t.token_bought_mint_address = g.mint_address
             AND t.token_sold_mint_address = 'So11111111111111111111111111111111111111112')
            OR
            (t.token_sold_mint_address = g.mint_address
             AND t.token_bought_mint_address = 'So11111111111111111111111111111111111111112')
        )
    WHERE LOWER(t.project) IN ('raydium', 'pumpswap', 'pump_swap')
)
SELECT
    g.mint_address,
    g.graduation_timestamp,
    t.block_time AS timestamp,
    CASE
        WHEN t.token_bought_mint_address = g.mint_address
            THEN t.token_sold_amount / NULLIF(t.token_bought_amount, 0)
        ELSE t.token_bought_amount / NULLIF(t.token_sold_amount, 0)
    END AS price_sol,
    CASE
        WHEN t.token_bought_mint_address = g.mint_address THEN 'buy'
        ELSE 'sell'
    END AS side,
    CASE
        WHEN t.token_bought_mint_address = g.mint_address
            THEN t.token_bought_amount
        ELSE t.token_sold_amount
    END AS token_amount,
    t.amount_usd AS amount_usd,
    LOWER(t.project) AS dex
FROM graduated_mints g
JOIN dex_solana.trades t
    ON t.block_time >= g.graduation_timestamp
    AND t.block_time < g.graduation_timestamp + INTERVAL '2' HOUR
    AND (
        (t.token_bought_mint_address = g.mint_address
         AND t.token_sold_mint_address = 'So11111111111111111111111111111111111111112')
        OR
        (t.token_sold_mint_address = g.mint_address
         AND t.token_bought_mint_address = 'So11111111111111111111111111111111111111112')
    )
WHERE LOWER(t.project) IN ('raydium', 'pumpswap', 'pump_swap')
ORDER BY g.mint_address, t.block_time;
