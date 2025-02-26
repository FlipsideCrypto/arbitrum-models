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
        END as amount_change
    FROM {{ ref('silver__pear_staking') }}
    WHERE action IN ('staked', 'unstaked')
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
),
-- look into accumulated claims vs unaccummulated claims --maybe make claimed stakes

-- fact table for referrals or as a referred by 
vol_tier as (
    SELECT *
    FROM (VALUES
        ('3',1000000, 0.05),
        ('2',3000000, 0.10),
        ('1',10000000, 0.15),
        ('platinum',20000000, 0.18)
    ) as t (rebate_tier, monthly_vol_requirement, rebate_rate)
)

SELECT
    date_trunc('month', sysdate()) as month,
    c.user_address,
    c.staked_amount,
    COALESCE(MAX(f.discount_rate), 0) as fee_discount_rate,
    COALESCE(MAX(v.rebate_rate), 0) as vol_rebate_rate,
    NULL AS monthly_volume,
    NULL as rebate_percentage,
    CASE 
        WHEN monthly_volume >= 20000000 THEN 'platinum'
        WHEN monthly_volume >= 10000000 THEN '1'
        WHEN monthly_volume >= 3000000 THEN '2'
        WHEN monthly_volume >= 1000000 THEN '3'
    ELSE NULL
    END AS rebate_tier,
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
LEFT JOIN vol_tier v 
    ON monthly_volume >= v.monthly_vol_requirement
GROUP BY 1,2,3
ORDER BY c.staked_amount DESC