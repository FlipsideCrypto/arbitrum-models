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
        event_name in ('DepositCreated','WithdrawalCreated')

),
decode_data AS (
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
        event_data[0][0][3][1] as market,
        event_data[0][0][4][1] as initial_long_token,
        event_data[0][0][5][1] as initial_short_token,
        event_data[1][0][0][1] as long_token_swap_path,
        event_data[1][0][1][1] as short_token_swap_path,
        event_data[2][0][0][1] as initial_long_token_amount,
        event_data[2][0][1][1] as initial_short_token_amount,
        event_data[2][0][2][1] as min_market_tokens,
        event_data[2][0][3][1] as updated_at_block,
        event_data[2][0][4][1] as execution_fee,
        event_data[2][0][5][1] as call_back_gas_limit,
        event_data[3][0][0][1] as should_unwrap_native_token,
        event_data[4][0][0][1] as key
    FROM
        gmx_events
),
contracts AS (
    SELECT
        contract_address,
        token_symbol,
        token_decimals
    FROM
        {{ ref('silver__contracts') }}
    WHERE
        contract_address IN (
            SELECT
                DISTINCT(initial_long_token)
            FROM
                decode_data
            UNION ALL
            SELECT
                DISTINCT(initial_short_token)
            FROM
                decode_data
        )
),
column_format AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        P.contract_address,
        event_index,
        _log_id,
        _inserted_timestamp,
        event_name,
        event_name_hash,
        msg_sender,
        topic_1,
        topic_2,
        account,
        receiver,
        market,
        call_back_contract,
        initial_long_token,
        c1.token_symbol AS initial_long_token_symbol,
        c1.token_decimals AS initial_long_token_decimals,
        initial_long_token_amount,
        initial_short_token,
        c2.token_symbol AS initial_short_token_symbol,
        c2.token_decimals AS initial_short_token_decimals,
        initial_short_token_amount,
        min_market_tokens,
        updated_at_block,
        execution_fee,
        call_back_gas_limit,
        should_unwrap_native_token,
        key
    FROM
        decode_data p 
    LEFT JOIN
        contracts c1
    on
        c1.contract_address = p.initial_long_token
    LEFT JOIN
        contracts c2
    on
        c2.contract_address = p.initial_short_token
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
    event_name,
    event_name_hash,
    market,    
    p.symbol,
    p.address as underlying_address,
    account,
    receiver,
    initial_long_token,
    initial_long_token_symbol,
    initial_long_token_amount as initial_long_token_amount_unadj,
    initial_long_token_amount :: INT / pow(
        10,
        initial_long_token_decimals
    ) AS initial_long_token_amount,
    initial_short_token,
    initial_short_token_symbol,
    initial_short_token_amount as initial_short_token_amount_unadj,
    initial_short_token_amount :: INT / pow(
        10,
        initial_short_token_decimals
    ) AS initial_short_token_amount,
    min_market_tokens,
    updated_at_block,
    execution_fee,
    call_back_gas_limit,
    should_unwrap_native_token,
    key,
    a._log_id,
    a._inserted_timestamp
FROM
    column_format a
LEFT JOIN
    {{ ref('silver__gmx_dim_products_v2') }} p 
ON
    p.market_address = a.market