{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH gmx_events AS (

    SELECT
        *
    FROM
        {{ ref('silver__gmx_events_v2') }}
    WHERE 
        event_name in ('OrderCreated','OrderExecuted')

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
),
lat_flat AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        event_index,
        _log_id,
        _inserted_timestamp,
        event_name,
        event_name_hash,
        msg_sender,
        topic_1,
        topic_2,
        event.value [0] :: variant AS event_data
    FROM
        gmx_events,
        LATERAL FLATTEN(
            input => event_data
        ) event
    WHERE
        event_name = 'OrderCreated'
),
lat_flat_2 AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        event_index,
        _log_id,
        _inserted_timestamp,
        event_name,
        event_name_hash,
        msg_sender,
        topic_1,
        topic_2,
        UPPER(
            VALUE [0] :: STRING
        ) AS key,
        VALUE [1] :: STRING AS VALUE
    FROM
        lat_flat,
        LATERAL FLATTEN (
            input => event_data
        )
),
pivot AS (
    SELECT
        *
    FROM
        lat_flat_2 pivot (MAX(VALUE) FOR key IN('ACCOUNT', 'RECEIVER', 'CALLBACKCONTRACT', 'UIFEERECEIVER', 'MARKET', 'INITIALCOLLATERALTOKEN', 'ORDERTYPE', 'DECREASEPOSITIONSWAPTYPE', 'SIZEDELTAUSD', 'INITIALCOLLATERALDELTAAMOUNT', 'TRIGGERPRICE', 'ACCEPTABLEPRICE', 'EXECUTIONFEE', 'CALLBACKGASLIMIT', 'MINOUTPUTAMOUNT', 'UPDATEDATBLOCK', 'ISLONG', 'SHOULDUNWRAPNATIVETOKEN', 'ISFROZEN', 'KEY'))
),
column_format AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        event_index,
        _log_id,
        _inserted_timestamp,
        event_name,
        event_name_hash,
        msg_sender,
        topic_1,
        topic_2,
        "'ACCOUNT'" AS account,
        "'RECEIVER'" AS reciever,
        "'CALLBACKCONTRACT'" AS callback_contract,
        "'UIFEERECEIVER'" AS ui_fee_reciever,
        "'MARKET'" AS market,
        "'INITIALCOLLATERALTOKEN'" AS initial_collateral_token,
        "'ORDERTYPE'" AS order_type,
        "'DECREASEPOSITIONSWAPTYPE'" AS decrease_position_swap_type,
        "'SIZEDELTAUSD'" AS size_delta_usd,
        "'INITIALCOLLATERALDELTAAMOUNT'" AS initial_collateral_delta_amount,
        "'TRIGGERPRICE'" AS trigger_price,
        "'ACCEPTABLEPRICE'" AS acceptable_price,
        "'EXECUTIONFEE'" execution_fee,
        "'CALLBACKGASLIMIT'" AS callback_gas_limit,
        "'MINOUTPUTAMOUNT'" AS min_output_amount,
        "'UPDATEDATBLOCK'" AS updated_at_block,
        "'ISLONG'" AS is_long,
        "'SHOULDUNWRAPNATIVETOKEN'" AS should_unwrap_native_token,
        "'ISFROZEN'" AS is_frozen,
        "'KEY'" AS key
    FROM
        pivot
),
executed_orders as (
select 
    event_data[4][0][0][1] as key
from 
    gmx_events
where 
    event_name = 'OrderExecuted'
)
SELECT
    a.block_number,
    a.block_timestamp,
    a.tx_hash,
    a.origin_function_signature,
    a.origin_from_address,
    a.origin_to_address,
    a.contract_address,
    a.event_index,
    account,
    reciever,
    callback_contract,
    ui_fee_reciever,
    market,
    p.symbol,
    p.address as underlying_address,
    event_name,
    event_name_hash,
    msg_sender,
    topic_1,
    topic_2,
    initial_collateral_token,
    order_type,
    CASE
        WHEN e.key is not null THEN 'executed'
        ELSE 'not-executed'
    END as order_execution,
    decrease_position_swap_type,
    size_delta_usd AS size_delta_usd_unadj,
    size_delta_usd :: FLOAT / pow(
        10,
        30
    ) AS size_delta_usd,
    initial_collateral_delta_amount AS initial_collateral_delta_amount_unadj,
    initial_collateral_delta_amount :: FLOAT / pow(
        10,
        6
    ) AS initial_collateral_delta_amount,
    trigger_price as trigger_price_unadj,
    trigger_price ::FLOAT / pow(
        10,
        12
    ) as trigger_price,
    acceptable_price as acceptable_price_unadj,
    acceptable_price ::FLOAT / pow(
        10,
        12
    ) AS acceptable_price,
    execution_fee as execution_fee_unadj,
    execution_fee ::FLOAT / pow(
        10,
        12
    ) AS execution_fee,
    callback_gas_limit,
    min_output_amount,
    updated_at_block,
    is_long,
    should_unwrap_native_token,
    is_frozen,
    a.key,
    a._log_id,
    a._inserted_timestamp
FROM
    column_format a
LEFT JOIN
    executed_orders e
ON
    a.key = e.key
LEFT JOIN
    {{ ref('silver__gmx_dim_products_v2') }} p 
ON
    p.market_address = a.market
