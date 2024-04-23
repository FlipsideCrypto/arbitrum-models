{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH vertex AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        event_name,
        event_index,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        trader,
        trade_type,
        'vertex' AS platform,
        REPLACE(
            symbol,
            '-PERP',
            ''
        ) AS symbol,
        market_reduce_flag,
        is_taker,
        price_amount_unadj,
        price_amount,
        amount_unadj,
        amount,
        amount_usd,
        fee_amount_unadj,
        fee_amount,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__vertex_perps') }}

{% if is_incremental() and 'vertex' not in var('HEAL_CURATED_MODEL') %}
WHERE
    A._inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) - INTERVAL '36 hours'
        FROM
            {{ this }}
    )
{% endif %}
),
gmx_v2 AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        event_name,
        event_index,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        account AS trader,
        CASE
            WHEN is_long = TRUE THEN 'buy/long'
            ELSE 'sell/short'
        END AS trade_type,
        'gmx-v2' AS platform,
        symbol,
        market_reduce_flag,
        CASE
            WHEN order_type IN (
                'market_increase',
                'market_decrease'
            ) THEN TRUE
            ELSE FALSE
        END AS is_taker,
        CASE
            WHEN is_taker = TRUE THEN acceptable_price_unadj
            WHEN is_taker = FALSE THEN trigger_price_unadj
        END AS price_amount_unadj,
        CASE
            WHEN is_taker = TRUE THEN acceptable_price
            WHEN is_taker = FALSE THEN trigger_price
        END AS price_amount,
        size_delta_usd_unadj :: FLOAT / NULLIF(
            price_amount_unadj,
            0
        ) AS amount_unadj,
        CASE
            WHEN trade_type = 'sell/short' THEN (size_delta_usd / NULLIF(price_amount, 0) * -1)
            ELSE size_delta_usd / NULLIF(
                price_amount,
                0
            )
        END AS amount,
        size_delta_usd AS amount_usd,
        NULL AS fee_amount_unadj,
        size_delta_usd * .0005 AS fee_amount,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__gmx_perps_v2') }}
    WHERE
        order_execution = 'executed'

{% if is_incremental() and 'gmx_v2' not in var('HEAL_CURATED_MODEL') %}
WHERE
    A._inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) - INTERVAL '36 hours'
        FROM
            {{ this }}
    )
{% endif %}
)
SELECT
    *
FROM
    vertex
UNION ALL
SELECT
    *
FROM
    gmx_v2
