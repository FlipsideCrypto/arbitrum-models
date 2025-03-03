{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'trader',
    tags = 'curated'
) }}

WITH

{% if is_incremental() %}
new_trader_actions AS (

    SELECT
        DISTINCT(trader)
    FROM
        {{ ref('silver__pear_perps') }}
    WHERE
        _inserted_timestamp >= (
            SELECT
                MAX(
                    _inserted_timestamp
                ) - INTERVAL '12 hours'
            FROM
                {{ this }}
        )
    UNION
    SELECT
        DISTINCT(trader)
    FROM
        {{ ref('silver__pear_liquidations') }}
    WHERE
        _inserted_timestamp >= (
            SELECT
                MAX(
                    _inserted_timestamp
                ) - INTERVAL '12 hours'
            FROM
                {{ this }}
        )
    UNION
    SELECT
        DISTINCT(trader)
    FROM
        {{this}}
    WHERE
        total_usd_volume_24h > 0
),
{% endif %}

trades_union AS (
    SELECT
        trader,
        trade_type,
        block_timestamp,
        platform,
        amount,
        amount_usd,
        fee_amount,
        pear_perps_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__pear_perps') }}

{% if is_incremental() %}
WHERE
    trader IN (
        SELECT
            trader
        FROM
            new_trader_actions
    )
{% endif %}
),
liquidations AS (
    SELECT
        trader,
        pear_liquidations_id,
        SUM(amount) AS total_liquidation_amount,
        COUNT(*) AS liquidation_count
    FROM
        {{ ref('silver__pear_liquidations') }}

{% if is_incremental() %}
WHERE
    trader IN (
        SELECT
            trader
        FROM
            new_trader_actions
    )
{% endif %}
GROUP BY
    trader,
    pear_liquidations_id
),
staking AS (
    SELECT 
        user_address as trader,
        SUM(CASE 
            WHEN action = 'staked' THEN amount 
            WHEN action = 'unstaked' THEN -amount
        END) as net_staked_amount
    FROM 
        {{ ref('silver__pear_staking') }}
    WHERE 
        action IN ('staked','unstaked')
    GROUP BY 
        1
),
FINAL AS (
    SELECT
        t.trader,
        MIN(t.block_timestamp) AS first_trade_timestamp,
        MAX(t.block_timestamp) AS last_trade_timestamp,
        DATEDIFF('day', first_trade_timestamp, last_trade_timestamp) AS account_age,
        COUNT(DISTINCT(t.pear_perps_id)) AS trade_count,
        COUNT(DISTINCT(CASE
            WHEN t.block_timestamp >= DATE_TRUNC('MONTH', CURRENT_DATE())
            THEN t.pear_perps_id
        END)) AS trade_count_mtd,
        SUM(
            CASE
                WHEN platform = 'gmx-v2' THEN + 1
                ELSE 0
            END
        ) AS gmx_trade_count,
        SUM(
            CASE
                WHEN platform = 'symmio' THEN + 1
                ELSE 0
            END
        ) AS symmio_trade_count,
        SUM(
            CASE
                WHEN platform = 'vertex' THEN + 1
                ELSE 0
            END
        ) AS vertex_trade_count,
        COUNT(*) AS perp_trade_count,
        SUM(
            CASE
                WHEN trade_type = 'buy/long' THEN + 1
                ELSE 0
            END
        ) AS long_count,
        SUM(
            CASE
                WHEN trade_type = 'sell/short' THEN + 1
                ELSE 0
            END
        ) AS short_count,
        SUM(amount_usd) AS total_usd_volume,
        SUM(
            CASE
                WHEN t.block_timestamp >= DATEADD('hour', -24, CURRENT_TIMESTAMP()) THEN amount_usd 
                ELSE 0
            END
        ) AS total_usd_volume_24h,
        SUM(
            CASE
                WHEN t.block_timestamp >= DATE_TRUNC('MONTH', CURRENT_DATE()) THEN amount_usd
                ELSE 0
            END
        ) AS total_usd_volume_mtd,
        AVG(amount_usd) AS avg_usd_trade_size,
        SUM(fee_amount) AS total_fee_amount,
        MAX(l.total_liquidation_amount) AS total_liquidation_amount,
        MAX(liquidation_count) AS total_liquidation_count,
        CASE
            WHEN total_usd_volume_mtd >  20000000 THEN .18
            WHEN total_usd_volume_mtd >  10000000 THEN .15
            WHEN total_usd_volume_mtd >  3000000 THEN .10
            WHEN total_usd_volume_mtd >  1000000 THEN .05
            ELSE 0
        END as current_rebate_rate,
        CASE
            WHEN total_usd_volume_mtd >  20000000 THEN 'platinum'
            WHEN total_usd_volume_mtd >  10000000 THEN '1'
            WHEN total_usd_volume_mtd >  3000000 THEN '2'
            WHEN total_usd_volume_mtd >  1000000 THEN '3'
            ELSE NULL
        END as current_rebate_tier,
        current_rebate_rate * total_fee_amount as estimated_rebate_amount,
        sum(s.net_staked_amount) as most_recent_staked_amount,
        CASE
            WHEN most_recent_staked_amount >  2500000 THEN .50
            WHEN most_recent_staked_amount >  1000000 THEN .40
            WHEN most_recent_staked_amount >  250000 THEN .30
            WHEN most_recent_staked_amount >  100000 THEN .20
            ELSE 0
        END as current_fee_discount_rate,
        MAX(t._inserted_timestamp) AS _inserted_timestamp
    FROM
        trades_union t
        LEFT JOIN liquidations l ON t.trader = l.trader
        LEFT JOIN staking s ON t.trader = s.trader
    GROUP BY
        t.trader
)
SELECT
    f.*,
    {{ dbt_utils.generate_surrogate_key(['f.trader']) }} AS pear_account_id,
    COALESCE(
    {% if is_incremental() %}
        a.inserted_timestamp,
    {% endif %}
        SYSDATE(),
        NULL
    ) AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL f
{% if is_incremental() %}
LEFT JOIN {{this}} a ON a.trader = f.trader
{% endif %}