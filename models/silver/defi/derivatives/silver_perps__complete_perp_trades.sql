{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg','heal']
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
        CASE 
            WHEN market_reduce_flag = TRUE THEN 'market_decrease'
            ELSE 'market_increase'
        END as market_type,
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

{% if is_incremental() and 'vertex' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
        FROM
            {{ this }}
    )
    AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
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
        CASE 
            WHEN market_reduce_flag = TRUE THEN 'market_decrease'
            ELSE 'market_increase'
        END as market_type,
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
            ELSE size_delta_usd / NULLIF(price_amount, 0)
        END AS amount,
        size_delta_usd AS amount_usd,
        null AS fee_amount_unadj,
        size_delta_usd * .0005 AS fee_amount,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver_perps__gmxv2_perps') }}
    WHERE
        order_execution = 'executed'

{% if is_incremental() and 'gmx_v2' not in var('HEAL_MODELS') %}
AND
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
        FROM
            {{ this }}
    )
    AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
),
symmio AS (
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
        party_a as trader,
        CASE 
            WHEN position_type = 0 THEN 'buy/long'
            WHEN position_type = 1 THEN 'sell/short'
            ELSE NULL 
        END as trade_type,
        'symmio' as platform,
        product_name as symbol,
        CASE 
            WHEN action_type = 'close' THEN 'market_decrease'
            ELSE 'market_increase'
        END as market_type,
        TRUE as is_taker, -- Adjust if this assumption is incorrect
        price as price_amount_unadj,
        price as price_amount, -- Adjust if decimals need scaling
        quantity as amount_unadj,
        CASE 
            WHEN position_type = 2 THEN quantity * -1
            ELSE quantity
        END as amount,
        quantity * price as amount_usd,
        trading_fee as fee_amount_unadj,
        trading_fee as fee_amount, -- Adjust if decimals need scaling
        _log_id,
        _inserted_timestamp
    FROM 
        {{ ref('silver_perps__symmio_perps') }}
    WHERE 
        status = 'filled'

{% if is_incremental() and 'symmio' not in var('HEAL_MODELS') %}
AND
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
        FROM
            {{ this }}
    )
    AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
),
perp_union as (
    SELECT *
    FROM
        vertex
    UNION ALL
    SELECT *
    FROM
        gmx_v2
    UNION ALL 
    SELECT *
    FROM
        symmio
)
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
    platform,
    symbol,
    market_type,
    is_taker,
    price_amount_unadj,
    price_amount,
    amount_unadj,
    amount,
    amount_usd,
    fee_amount_unadj,
    fee_amount,
    _log_id,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'event_index']
    ) }} as complete_perp_trades_id,
    SYSDATE() as modified_timestamp,
    SYSDATE() as inserted_timestamp,
    '{{invocation_id}}' as _invocation_id
FROM
    perp_union