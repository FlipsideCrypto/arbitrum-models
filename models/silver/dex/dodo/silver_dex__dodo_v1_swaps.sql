{{ config(
    materialized = 'incremental',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['non_realtime']
) }}

WITH pools AS (

    SELECT
        '0xfe176a2b1e1f67250d2903b8d25f56c0dabcd6b2' AS pool_address,
        'WETH' AS base_token_symbol,
        'USDC' AS quote_token_symbol,
        '0x82af49447d8a07e3bd95bd0d56f35241523fbab1' AS base_token,
        '0xff970a61a04b1ca14834a43f5de4533ebddb5cc8' AS quote_token
    UNION
    SELECT
        '0xe4b2dfc82977dd2dce7e8d37895a6a8f50cbb4fb', 
        'USDT',
        'USDC',
        '0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9',
        '0xff970a61a04b1ca14834a43f5de4533ebddb5cc8'
    UNION
    SELECT
        '0xb42a054d950dafd872808b3c839fbb7afb86e14c',
        'WBTC',
        'USDC',
        '0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f',
        '0xff970a61a04b1ca14834a43f5de4533ebddb5cc8'
),  
proxies AS (
    SELECT
        '0xd5a7e197bace1f3b26e2760321d6ce06ad07281a' AS proxy_address
    UNION
    SELECT
        '0x8ab2d334ce64b50be9ab04184f7ccba2a6bb6391' AS proxy_address
),
sell_base_token AS (
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
        CONCAT('0x', SUBSTR(l.topics [1] :: STRING, 27, 40)) AS seller_address,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                l_segmented_data [0] :: STRING
            )
        ) AS payBase,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                l_segmented_data [1] :: STRING
            )
        ) AS receiveQuote,
        base_token,
        quote_token,
        quote_token AS tokenIn,
        base_token AS tokenOut,
        receiveQuote AS amountIn,
        payBase AS amountOut,
        l._log_id,
        l._inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
        l
        INNER JOIN pools p
        ON p.pool_address = l.contract_address
    WHERE
        topics [0] :: STRING = '0xd8648b6ac54162763c86fd54bf2005af8ecd2f9cb273a5775921fd7f91e17b2d' --sellBaseToken
        AND seller_address NOT IN (
            SELECT
                proxy_address
            FROM
                proxies
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) :: DATE
    FROM
        {{ this }}
)
{% endif %}
),
buy_base_token AS (
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
        CONCAT('0x', SUBSTR(l.topics [1] :: STRING, 27, 40)) AS buyer_address,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                l_segmented_data [0] :: STRING
            )
        ) AS receiveBase,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                l_segmented_data [1] :: STRING
            )
        ) AS payQuote,
        base_token,
        quote_token,
        quote_token AS tokenIn,
        base_token AS tokenOut,
        payQuote AS amountIn,
        receiveBase AS amountOut,
        l._log_id,
        l._inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
        l
        INNER JOIN pools p
        ON p.pool_address = l.contract_address
    WHERE
        topics [0] :: STRING = '0xe93ad76094f247c0dafc1c61adc2187de1ac2738f7a3b49cb20b2263420251a3' --buyBaseToken
        AND buyer_address NOT IN (
            SELECT
                proxy_address
            FROM
                proxies
        )

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
    seller_address AS sender,
    origin_from_address AS tx_to,
    tokenIn AS token_in,
    tokenOut AS token_out,
    amountIn AS amount_in_unadj,
    amountOut AS amount_out_unadj,
    'SellBaseToken' AS event_name,
    'dodo-v1' AS platform,
    _log_id,
    _inserted_timestamp
FROM
    sell_base_token
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
    buyer_address AS sender,
    origin_from_address AS tx_to,
    tokenIn AS token_in,
    tokenOut AS token_out,
    amountIn AS amount_in_unadj,
    amountOut AS amount_out_unadj,
    'BuyBaseToken' AS event_name,
    'dodo-v1' AS platform,
    _log_id,
    _inserted_timestamp
FROM
    buy_base_token
