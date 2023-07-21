{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH swaps_base AS (

    SELECT
        l.block_number,
        l.block_timestamp,
        l.tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        l.event_index,
        l.contract_address,
        regexp_substr_all(SUBSTR(l.data, 3, len(l.data)), '.{64}') AS l_segmented_data,
        CONCAT(
            '0x',
            SUBSTR(
                l_segmented_data [0] :: STRING,
                25,
                40
            )
        ) AS account_address,
        CONCAT(
            '0x',
            SUBSTR(
                l_segmented_data [1] :: STRING,
                25,
                40
            )
        ) AS tokenIn,
        CONCAT(
            '0x',
            SUBSTR(
                l_segmented_data [2] :: STRING,
                25,
                40
            )
        ) AS tokenOut,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                l_segmented_data [3] :: STRING
            )
        ) AS amountIn,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                l_segmented_data [4] :: STRING
            )
        ) AS amountOut,
        l._log_id,
        l._inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
        l
    WHERE
        contract_address IN (
            '0xabbc5f99639c9b6bcb58544ddf04efa6802f4064'
        )
        AND topics [0] :: STRING = '0xcd3829a3813dc3cdd188fd3d01dcf3268c16be2fdd2dd21d0665418816e46062' --Swap

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    event_index,
    contract_address,
    origin_from_address AS sender,
    account_address AS tx_to,
    tokenIn AS token_in,
    tokenOut AS token_out,
    amountIn AS amount_in_unadj,
    amountOut AS amount_out_unadj,
    'Swap' AS event_name,
    'gmx' AS platform,
    _log_id,
    _inserted_timestamp
FROM
    swaps_base