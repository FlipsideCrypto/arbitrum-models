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
parse_data AS (
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
        event_data[0][0][0][1] as account,
        event_data[0][0][1][1] as receiver,
        event_data[0][0][2][1] as call_back_contract,
        event_data[0][0][3][1] as ui_fee_receiver,
        event_data[0][0][4][1] as market,
        event_data[0][0][5][1] as initial_collateral_token,
        event_data[0][1][0][1][0] as swap_path,
        event_data[1][0][0][1] as order_type,
        event_data[1][0][1][1] as decrease_position_swap_type,
        event_data[1][0][2][1] as size_delta_usd,
        event_data[1][0][3][1] as initial_collateral_delta_amount,
        event_data[1][0][4][1] as trigger_price,
        event_data[1][0][5][1] as acceptable_price,
        event_data[1][0][6][1] as execution_fee,
        event_data[1][0][7][1] as call_back_gas_limit,
        event_data[1][0][8][1] as min_output_amount,
        event_data[1][0][9][1] as updated_at_block,
        event_data[3][0][0][1] as is_long,
        event_data[3][0][1][1] as should_unwrap_native_token,
        event_data[3][0][2][1] as is_frozen,
        event_data[4][0][0][1] as key,
    FROM
        gmx_events
    WHERE
        event_name = 'OrderCreated'
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
    receiver,
    call_back_contract,
    ui_fee_receiver,
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
    call_back_gas_limit,
    min_output_amount,
    updated_at_block,
    is_long,
    should_unwrap_native_token,
    is_frozen,
    a.key,
    a._log_id,
    a._inserted_timestamp
FROM
    parse_data a
LEFT JOIN
    executed_orders e
ON
    a.key = e.key
LEFT JOIN
    {{ ref('silver__gmx_dim_products_v2') }} p 
ON
    p.market_address = a.market
