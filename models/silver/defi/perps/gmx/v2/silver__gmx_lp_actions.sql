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
)
,
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
        lat_flat_2 pivot (MAX(VALUE) FOR key IN(
            'ACCOUNT',
            'RECEIVER',
            'MARKET',
            'CALLBACKCONTRACT',
            'INITIALLONGTOKEN',
            'INITIALLONGTOKENAMOUNT',
            'INITIALSHORTTOKEN',
            'INITIALSHORTTOKENAMOUNT',
            'MINMARKETTOKENS',
            'UPDATEDATBLOCK',
            'EXECUTIONFEE',
            'CALLBACKGASLIMIT',
            'SHOULDUNWRAPNATIVETOKEN',
            'KEY'
        )
    )
)
,
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
                DISTINCT("'INITIALLONGTOKEN'")
            FROM
                pivot
            UNION ALL
            SELECT
                DISTINCT("'INITIALSHORTTOKEN'")
            FROM
                pivot
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
        "'ACCOUNT'" AS account,
        "'RECEIVER'" AS reciever,
        "'MARKET'" AS market,
        "'CALLBACKCONTRACT'" AS call_back_contract,
        "'INITIALLONGTOKEN'" AS initial_long_token,
        c1.token_symbol AS initial_long_token_symbol,
        c1.token_decimals AS initial_long_token_decimals,
        "'INITIALLONGTOKENAMOUNT'" AS initial_long_token_amount,
        "'INITIALSHORTTOKEN'" AS initial_short_token,
        c2.token_symbol AS initial_short_token_symbol,
        c2.token_decimals AS initial_short_token_decimals,
        "'INITIALSHORTTOKENAMOUNT'" AS initial_short_token_amount,
        "'MINMARKETTOKENS'" AS min_market_tokens,
        "'UPDATEDATBLOCK'" AS updated_block,
        "'EXECUTIONFEE'" AS execution_fee,
        "'CALLBACKGASLIMIT'" AS call_back_gas_limit,
        "'SHOULDUNWRAPNATIVETOKEN'" AS should_unwrap_native_token,
        "'KEY'" AS key
    FROM
        pivot p 
    LEFT JOIN
        contracts c1
    on
        c1.contract_address = p."'INITIALLONGTOKEN'"
    LEFT JOIN
        contracts c2
    on
        c2.contract_address = p."'INITIALSHORTTOKEN'"
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
    reciever,
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
    updated_block,
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