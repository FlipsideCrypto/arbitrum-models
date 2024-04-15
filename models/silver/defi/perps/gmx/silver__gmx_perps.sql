{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH decoded_logs AS (

    SELECT
        *
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        topics [0] :: STRING = '0x468a25a7ba624ceea6e540ad6f49171b52495b648417ae91bca21676d8a24dc5'
        AND origin_to_address = '0x7c68c7866a64fa2160f78eeae12217ffbf871fa8'

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
        event.value [0] :: variant AS event_data
    FROM
        decoded_logs,
        LATERAL FLATTEN(
            input => decoded_flat :eventData
        ) event
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
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    event_index,
    account,
    reciever,
    callback_contract,
    ui_fee_reciever,
    market,
    initial_collateral_token,
    order_type,
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
    key,
    _log_id,
    _inserted_timestamp
FROM
    column_format
