{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['month', 'user_address'],
    tags = ['curated']
) }}

WITH staking_actions AS (
    SELECT 
        user_address,
        block_timestamp,
        CASE 
            WHEN action = 'staked' THEN amount
            WHEN action = 'unstaked' THEN -amount
            WHEN action = 'claim-compound' THEN amount
        END as amount_change
    FROM {{ ref('silver__pear_staking') }}
    WHERE action IN ('staked', 'unstaked', 'claim-compound')
    {% if is_incremental() %}
    AND modified_timestamp >= (
        SELECT
            MAX(modified_timestamp) - INTERVAL '12 hours'
        FROM
            {{ this }}
    )
    {% endif %}
),

current_stakes AS (
    SELECT
        user_address,
        SUM(amount_change) as staked_amount
    FROM staking_actions
    GROUP BY user_address
    HAVING staked_amount > 0
),

fee_tiers AS (
    SELECT *
    FROM (VALUES
        (10000, 0.05),
        (50000, 0.10),
        (100000, 0.20),
        (250000, 0.30),
        (1000000, 0.40),
        (2500000, 0.50)
    ) as t (stake_requirement, discount_rate)
)

SELECT
    date_trunc('month', sysdate()) as month,
    c.user_address,
    c.staked_amount,
    COALESCE(MAX(f.discount_rate), 0) as fee_discount_rate,
    NULL AS monthly_volume,
    NULL as rebate_percentage,
    NULL as rebate_tier,
    NULL AS estimated_rebate_amount,
    SYSDATE() as inserted_timestamp,
    SYSDATE() as modified_timestamp,
    '{{ invocation_id }}' as _invocation_id,
    {{ dbt_utils.generate_surrogate_key(
        ['c.user_address']
    ) }} as pear_fee_discount_id
FROM current_stakes c
LEFT JOIN fee_tiers f 
    ON c.staked_amount >= f.stake_requirement
GROUP BY 1,2,3
ORDER BY c.staked_amount DESC