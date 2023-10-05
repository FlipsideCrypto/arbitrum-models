{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['non_realtime','reorg']
) }}

WITH router_swaps_base AS (

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
        CONCAT('0x', SUBSTR(l.topics [1] :: STRING, 27, 40)) AS from_token,
        CONCAT('0x', SUBSTR(l.topics [2] :: STRING, 27, 40)) AS to_token,
        CONCAT('0x', SUBSTR(l.topics [3] :: STRING, 27, 40)) AS to_address,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                l_segmented_data [0] :: STRING
            )
        ) AS swapType,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                l_segmented_data [1] :: STRING
            )
        ) AS fromAmount,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                l_segmented_data [2] :: STRING
            )
        ) AS toAmount,
        CONCAT(
            '0x',
            SUBSTR(
                l_segmented_data [3] :: STRING,
                25,
                40
            )
        ) AS from_address,
        CONCAT(
            '0x',
            SUBSTR(
                l_segmented_data [4] :: STRING,
                25,
                40
            )
        ) AS rebateTo,
        l._log_id,
        l._inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
        l
    WHERE
        contract_address IN (
            '0xcdfd61a8303beb5c8dd2a6d02df8d228ce15b9f3',
            '0x9aed3a8896a85fe9a8cac52c9b402d092b629a30',
            '0xd2635bc7e4e4f63b2892ed80d0b0f9dff7eda899',
            --v2
            '0xb130a49065178465931d4f887056328cea5d723f'
        ) --v3
        AND topics [0] :: STRING = '0x27c98e911efdd224f4002f6cd831c3ad0d2759ee176f9ee8466d95826af22a1c' --WooRouterSwap

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
),
swaps_base AS (
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
        CONCAT('0x', SUBSTR(l.topics [1] :: STRING, 27, 40)) AS from_token,
        CONCAT('0x', SUBSTR(l.topics [2] :: STRING, 27, 40)) AS to_token,
        CONCAT('0x', SUBSTR(l.topics [3] :: STRING, 27, 40)) AS to_address,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                l_segmented_data [0] :: STRING
            )
        ) AS fromAmount,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                l_segmented_data [1] :: STRING
            )
        ) AS toAmount,
        CONCAT(
            '0x',
            SUBSTR(
                l_segmented_data [2] :: STRING,
                25,
                40
            )
        ) AS from_address,
        CONCAT(
            '0x',
            SUBSTR(
                l_segmented_data [3] :: STRING,
                25,
                40
            )
        ) AS rebateTo,
        l._log_id,
        l._inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
        l
    WHERE
        contract_address IN (
            '0x39d361e66798155813b907a70d6c2e3fdafb0877',
            '0xc04362cf21e6285e295240e30c056511df224cf4',
            '0x86b1742a1d7c963d3e8985829d722725316abf0a',
            '0xeff23b4be1091b53205e35f3afcd9c7182bf3062',
            '0xb89a33227876aef02a7ebd594af9973aece2f521',
            '0x8693f9701d6db361fe9cc15bc455ef4366e39ae0',
            '0x1f79f8a65e02f8a137ce7f79c038cc44332df448'
        )
        AND topics [0] :: STRING IN (
            '0x74ef34e2ea7c5d9f7b7ed44e97ad44b4303416c3a660c3fb5b3bdb95a1d6abd3',
            '0x0e8e403c2d36126272b08c75823e988381d9dc47f2f0a9a080d95f891d95c469'
        ) --WooSwap
        AND tx_hash NOT IN (
            SELECT
                tx_hash
            FROM
                router_swaps_base
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
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
    CASE
        WHEN from_token = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0x82af49447d8a07e3bd95bd0d56f35241523fbab1'
        ELSE from_token
    END AS token_in,
    CASE
        WHEN to_token = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0x82af49447d8a07e3bd95bd0d56f35241523fbab1'
        ELSE to_token
    END AS token_out,
    to_address AS tx_to,
    swapType AS swap_type,
    fromAmount AS amount_in_unadj,
    toAmount AS amount_out_unadj,
    from_address AS sender,
    rebateTo AS rebate_to,
    'WooRouterSwap' AS event_name,
    'woofi' AS platform,
    _log_id,
    _inserted_timestamp
FROM
    router_swaps_base
UNION ALL
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    event_index,
    contract_address,
    CASE
        WHEN from_token = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0x82af49447d8a07e3bd95bd0d56f35241523fbab1'
        ELSE from_token
    END AS token_in,
    CASE
        WHEN to_token = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0x82af49447d8a07e3bd95bd0d56f35241523fbab1'
        ELSE to_token
    END AS token_out,
    to_address AS tx_to,
    NULL AS swap_type,
    fromAmount AS amount_in_unadj,
    toAmount AS amount_out_unadj,
    from_address AS sender,
    rebateTo AS rebate_to,
    'WooSwap' AS event_name,
    'woofi' AS platform,
    _log_id,
    _inserted_timestamp
FROM
    swaps_base
