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
        -- market_type  (always decrease only?)
        trader,
        digest AS liquidator,
        CASE
            WHEN amount < 0 THEN 'sell/short'
            WHEN amount > 0 THEN 'buy/long'
        END AS trade_type,
        'vertex' AS platform,
        FALSE AS is_taker,
        health_group_symbol AS symbol,
        amount_quote_unadj / amount_unadj AS price_amount_unadj,
        amount_quote / amount AS price_amount,
        amount_unadj AS liquidated_amount_unadj,
        amount AS liquidated_amount,
        amount_quote AS liquidated_amount_usd,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__vertex_liquidations') }}

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
        -- market_type  (always decrease only?)
        account AS trader,
        origin_from_address AS liquidator,
        -- GMX LiquidationHandler
        CASE
            WHEN is_long = TRUE THEN 'buy/long'
            ELSE 'sell/short'
        END AS trade_type,
        'gmx-v2' AS platform,
        FALSE AS is_taker,
        symbol,
        execution_price_unadj AS price_amount_unadj,
        execution_price AS price_amount,
        size_delta_amount_unadj AS liquidated_amount_unadj,
        size_delta_amount AS liquidated_amount,
        size_delta_usd AS liquidated_amount_usd,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver_perps__gmxv2_liquidations') }}

{% if is_incremental() and 'gmx_v2' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
        FROM
            {{ this }}
    )
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
        partyA AS trader,
        liquidator,
        CASE
            WHEN position_type = 0 THEN 'buy/long'
            WHEN position_type = 1 THEN 'sell/short'
            ELSE NULL
        END AS trade_type,
        'symmio' AS platform,
        FALSE AS is_taker,
        product_name AS symbol,
        price AS price_amount_unadj,
        price AS price_amount,
        liquidated_amount_unadj,
        liquidated_amount,
        -- token quantity
        liquidated_amount_usd,
        -- in usd value
        _log_id,
       modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('silver_perps__symmio_liquidations') }}

{% if is_incremental() and 'symmio' not in var('HEAL_MODELS') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
        FROM
            {{ this }}
    )
{% endif %}
),

liq_union as (
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
    liquidator,
    trade_type,
    platform,
    is_taker,
    symbol,
    price_amount_unadj,
    price_amount,
    liquidated_amount_unadj,
    liquidated_amount,
    liquidated_amount_usd,
    _log_id,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'event_index']
    ) }} as perp_liquidations_id,
    SYSDATE() as modified_timestamp,
    SYSDATE() as inserted_timestamp,
    '{{invocation_id}}' as _invocation_id
FROM
    liq_union