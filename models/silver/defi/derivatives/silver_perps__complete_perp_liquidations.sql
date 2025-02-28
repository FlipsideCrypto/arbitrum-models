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
        digest as liquidator,
        CASE
            WHEN amount < 0 THEN 'sell/short'
            WHEN amount > 0 THEN 'buy/long'
        END AS trade_type,
        'vertex' AS platform,
        FALSE as is_taker, 
        health_group_symbol as symbol,
        amount_quote_unadj/amount_unadj as price_amount_unadj,
        amount_quote/amount as price_amount,
        amount_unadj as liquidated_amount_unadj,
        amount as liquidated_amount,
        amount_quote as liquidated_amount_usd,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__vertex_liquidations') }}

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
        -- market_type  (always decrease only?)
        account AS trader,
        origin_from_address as liquidator, -- GMX LiquidationHandler
        CASE
            WHEN is_long = TRUE THEN 'buy/long'
            ELSE 'sell/short'
        END AS trade_type,
        'gmx-v2' AS platform,
        FALSE as is_taker,
        symbol,
        execution_price_unadj as price_amount_unadj,
        execution_price as price_amount,
        size_delta_usd_unadj as liquidated_amount_unadj,
        size_delta_usd as liquidated_amount,
        size_delta_usd_unadj as liquidated_amount_usd,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver_perps__gmxv2_liquidations') }}

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
        partyA as trader,
        liquidator,
        CASE 
            WHEN position_type = 0 THEN 'buy/long'
            WHEN position_type = 1 THEN 'sell/short'
            ELSE NULL 
        END as trade_type,
        'symmio' as platform,
        FALSE as is_taker,
        product_name as symbol,
        price as price_amount_unadj,
        price as price_amount,
        liquidatedAmount_unadj AS liquidated_amount_unadj,
        liquidatedAmount AS liquidated_amount, -- token quantity
        liquidatedAmount_usd AS liquidated_amount_usd, -- in usd value
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver_perps__symmio_liquidations') }}

{% if is_incremental() and 'symmio' not in var('HEAL_CURATED_MODEL') %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '36 hours'
    FROM
        {{ this }}
)
{% endif %}
)
