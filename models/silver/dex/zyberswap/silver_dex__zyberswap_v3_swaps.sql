{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH pools AS (

    SELECT
        pool_address,
        token0,
        token1
    FROM
        {{ ref('silver_dex__zyberswap_pools') }}
),
swaps_base AS (
    SELECT
        block_number,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        block_timestamp,
        tx_hash,
        event_index,
        contract_address,
        regexp_substr_all(SUBSTR(l.data, 3, len(l.data)), '.{64}') AS l_segmented_data,
        CONCAT('0x', SUBSTR(l.topics [1] :: STRING, 27, 40)) AS sender_address,
        CONCAT('0x', SUBSTR(l.topics [2] :: STRING, 27, 40)) AS reipient_address,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                's2c',
                l_segmented_data [0] :: STRING
            )
        ) AS amount0,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                's2c',
                l_segmented_data [1] :: STRING
            )
        ) AS amount1,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                l_segmented_data [2] :: STRING
            )
        ) AS sqrtP,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                l_segmented_data [3] :: STRING
            )
        ) AS liquidity,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                's2c',
                l_segmented_data [4] :: STRING
            )
        ) AS currentTick,
        ABS(GREATEST(amount0, amount1)) AS amountOut,
        ABS(LEAST(amount0, amount1)) AS amountIn,
        token0,
        token1,
        CASE
            WHEN amount0 < 0 THEN token0
            ELSE token1
        END AS token_in,
        CASE
            WHEN amount0 > 0 THEN token0
            ELSE token1
        END AS token_out,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }} l
        INNER JOIN pools p
        ON p.pool_address = contract_address
    WHERE
        topics [0] :: STRING = '0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67'
        AND tx_status = 'SUCCESS'

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
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    tx_hash,
    event_index,
    contract_address,
    sender_address AS sender,
    reipient_address AS tx_to,
    amount0,
    amount1,
    sqrtP,
    liquidity,
    currentTick,
    amountIn AS amount_in_unadj,
    amountOut AS amount_out_unadj,
    token_in,
    token_out,
    'Swap' AS event_name,
    'zyberswap-v3' AS platform,
    _log_id,
    _inserted_timestamp
FROM
    swaps_base
