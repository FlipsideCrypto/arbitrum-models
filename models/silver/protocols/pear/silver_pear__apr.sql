{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'DAY',
    cluster_by = ['DAY::DATE'],
    tags = ['curated','reorg']
) }}

WITH depositstakerfee AS (

    SELECT
        block_timestamp,
        tx_hash,
        TRY_TO_NUMBER(
            decoded_log :feeAmount :: STRING
        ) AS feeAmount,
        TRY_TO_NUMBER(
            decoded_log :feeAmountEarnedPerToken :: STRING
        ) AS feeAmountEarnedPerToken,
        CONCAT(
            tx_hash,
            '-',
            event_index
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__ez_decoded_event_logs') }}
    WHERE
        contract_address = '0xce3be5204017bb1bd279937f92df09fd7f539b92'
        AND topic_0 = '0xb454b7a6025b5d6560b4eca87d2db887d0fc8e1223331c4261e4ea7eb7439838'
        AND tx_succeeded

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
),
daily_staker_fee AS (
    SELECT
        DATE(block_timestamp) AS DAY,
        SUM(feeAmount) AS daily_fee_unadj,
        SUM(feeAmountEarnedPerToken) AS daily_fee_per_token_unadj
    FROM
        depositstakerfee
    GROUP BY
        DAY
),
daily_price AS (
    SELECT
        DATE(HOUR) AS DAY,
        token_address,
        AVG(price) AS avg_price
    FROM
        {{ ref('price__ez_prices_hourly') }}
    WHERE
        token_address IN (
            '0x82af49447d8a07e3bd95bd0d56f35241523fbab1',-- weth
            '0x3212dc0f8c834e4de893532d27cc9b6001684db0' -- pear
        )
    GROUP BY
        DAY,
        token_address
)
SELECT
    DAY,
    daily_fee_unadj,
    daily_fee_unadj * 100 * pow(
        10,
        -18
    ) AS daily_fee,
    daily_fee * p1.avg_price AS daily_fee_usd,
    'ETH' as fee_symbol,
    daily_fee_per_token_unadj,
    daily_fee_per_token_unadj * 100 * pow(
        10,
        -18
    ) AS daily_fee_per_token,
    (
        p1.avg_price * daily_fee_per_token_unadj * 100 * pow(
            10,
            -18
        ) * 365
    ) / p2.avg_price AS apr,
    {{ dbt_utils.generate_surrogate_key(
    ['day']
    ) }} AS pear_daily_apr_id,
    _inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    daily_staker_fee
    LEFT JOIN daily_price p1 USING(DAY)
    LEFT JOIN daily_price p2 USING(DAY)
WHERE
    p1.token_address = '0x82af49447d8a07e3bd95bd0d56f35241523fbab1'
    AND p2.token_address = '0x3212dc0f8c834e4de893532d27cc9b6001684db0'
