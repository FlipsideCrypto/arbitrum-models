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
        product_name as symbol,
        FALSE as is_taker,
        price as price_amount_unadj,
        price as price_amount,
        liquidatedAmount_unadj AS liquidated_amount_unadj,
        liquidatedAmount AS liquidated_amount,
        liquidatedAmount_usd AS liquidated_amount_usd,
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
