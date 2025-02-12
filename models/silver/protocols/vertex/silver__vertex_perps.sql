{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH vertex_products AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        product_id :: STRING AS product_id,
        book_address,
        product_type,
        ticker_id,
        symbol,
        NAME,
        version
    FROM
        {{ ref('silver__vertex_dim_products') }}
    WHERE
        product_type = 'perp'
),
logs AS (
    SELECT
        *
    FROM
        {{ ref('silver__logs') }}
    WHERE
        (
            contract_address IN (
                SELECT
                    book_address
                FROM
                    vertex_products
                WHERE
                    product_type = 'perp'
            )
            OR contract_address = LOWER('0xa4369d8e3dc847aedf17f4125f1abb1bc18fc060')
        )
{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'

{% endif %}
),
order_fill_decode AS (
    SELECT
        l.block_number,
        l.block_timestamp,
        l.tx_hash,
        l.contract_address,
        'FillOrder' AS event_name,
        event_index,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        symbol,
        s.product_id,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        topics [1] :: STRING AS digest,
        --unique hash of the order
        LEFT(
            topics [2] :: STRING,
            42
        ) AS trader,
        topics [2] :: STRING AS subaccount,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) :: INT AS pricex18,
        utils.udf_hex_to_int(
            's2c',
            segmented_data [1] :: STRING
        ) :: INT AS amount,
        utils.udf_hex_to_int(
            segmented_data [2] :: STRING
        ) AS expiration,
        utils.udf_hex_to_int(
            segmented_data [3] :: STRING
        ) :: INT AS nonce,
        utils.udf_hex_to_int(
            's2c',
            segmented_data [4] :: STRING
        ) :: INT AS isTaker,
        utils.udf_hex_to_int(
            's2c',
            segmented_data [5] :: STRING
        ) :: INT AS feeAmount,
        utils.udf_hex_to_int(
            's2c',
            segmented_data [6] :: STRING
        ) :: INT AS baseDelta,
        utils.udf_hex_to_int(
            's2c',
            segmented_data [7] :: STRING
        ) :: INT AS quoteDelta,
        l._log_id,
        l._inserted_timestamp
    FROM
        logs l
        INNER JOIN vertex_products s
        ON s.book_address = l.contract_address
    WHERE
        topics [0] :: STRING = '0x224253ad5cda2459ff587f559a41374ab9243acbd2daff8c13f05473db79d14c'
),
order_fill_decode_v2 AS (
    SELECT
        l.block_number,
        l.block_timestamp,
        l.tx_hash,
        l.contract_address,
        'FillOrder' AS event_name,
        event_index,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        utils.udf_hex_to_int(
            topics [1] :: STRING
        ) :: STRING AS product_id,
        s.symbol,
        topics [2] :: STRING AS digest,
        --unique hash of the order
        LEFT(
            topics [3] :: STRING,
            42
        ) AS trader,
        topics [3] :: STRING AS subaccount,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) :: INT AS pricex18,
        utils.udf_hex_to_int(
            's2c',
            segmented_data [1] :: STRING
        ) :: INT AS amount,
        utils.udf_hex_to_int(
            segmented_data [2] :: STRING
        ) AS expiration,
        utils.udf_hex_to_int(
            segmented_data [3] :: STRING
        ) :: INT AS nonce,
        utils.udf_hex_to_int(
            's2c',
            segmented_data [4] :: STRING
        ) :: INT AS isTaker,
        utils.udf_hex_to_int(
            's2c',
            segmented_data [5] :: STRING
        ) :: INT AS feeAmount,
        utils.udf_hex_to_int(
            's2c',
            segmented_data [6] :: STRING
        ) :: INT AS baseDelta,
        utils.udf_hex_to_int(
            's2c',
            segmented_data [7] :: STRING
        ) :: INT AS quoteDelta,
        l._log_id,
        l._inserted_timestamp
    FROM
        logs l
        INNER JOIN vertex_products s
        ON s.product_id = utils.udf_hex_to_int(
            topics [1] :: STRING
        )
    WHERE
        topics [0] :: STRING = '0x7c57459d6f4f0fb2fc5b1e298c8c0eb238422944964aa1e249eaa78747f0cca9'
        AND contract_address = LOWER('0xa4369d8e3dc847aedf17f4125f1abb1bc18fc060')
),
order_fill_format AS (
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
        symbol,
        product_id,
        digest,
        trader,
        subaccount,
        'v1' AS version,
        expiration AS expiration_raw,
        utils.udf_int_to_binary(TRY_TO_NUMBER(expiration)) AS exp_binary,
        utils.udf_binary_to_int(SUBSTR(exp_binary, -2)) AS order_type,
        utils.udf_binary_to_int(SUBSTR(exp_binary, -3, 1)) AS market_reduce_flag,
        CASE
            WHEN len(expiration) < 11 THEN TRY_TO_TIMESTAMP(utils.udf_binary_to_int(exp_binary) :: STRING)
            ELSE TRY_TO_TIMESTAMP(
                utils.udf_binary_to_int(SUBSTR(exp_binary, 24)) :: STRING
            )
        END AS expiration,
        nonce,
        isTaker,
        feeAmount AS fee_amount_unadj,
        feeAmount / pow(
            10,
            18
        ) AS fee_amount,
        pricex18 AS price_amount_unadj,
        pricex18 / pow(
            10,
            18
        ) AS price_amount,
        amount AS amount_unadj,
        amount / pow(
            10,
            18
        ) AS amount,
        baseDelta AS base_delta_unadj,
        baseDelta / pow(
            10,
            18
        ) AS base_delta,
        quoteDelta AS quote_delta_unadj,
        quoteDelta / pow(
            10,
            18
        ) AS quote_delta,
        _log_id,
        _inserted_timestamp
    FROM
        order_fill_decode
    UNION ALL
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
        symbol,
        product_id,
        digest,
        trader,
        subaccount,
        'v2' AS version,
        expiration AS expiration_raw,
        utils.udf_int_to_binary(TRY_TO_NUMBER(expiration)) AS exp_binary,
        utils.udf_binary_to_int(SUBSTR(exp_binary, -2)) AS order_type,
        utils.udf_binary_to_int(SUBSTR(exp_binary, -3, 1)) AS market_reduce_flag,
        CASE
            WHEN len(expiration) < 11 THEN TRY_TO_TIMESTAMP(utils.udf_binary_to_int(exp_binary) :: STRING)
            ELSE TRY_TO_TIMESTAMP(
                utils.udf_binary_to_int(SUBSTR(exp_binary, 24)) :: STRING
            )
        END AS expiration,
        nonce,
        isTaker,
        feeAmount AS fee_amount_unadj,
        feeAmount / pow(
            10,
            18
        ) AS fee_amount,
        pricex18 AS price_amount_unadj,
        pricex18 / pow(
            10,
            18
        ) AS price_amount,
        amount AS amount_unadj,
        amount / pow(
            10,
            18
        ) AS amount,
        baseDelta AS base_delta_unadj,
        baseDelta / pow(
            10,
            18
        ) AS base_delta,
        quoteDelta AS quote_delta_unadj,
        quoteDelta / pow(
            10,
            18
        ) AS quote_delta,
        _log_id,
        _inserted_timestamp
    FROM
        order_fill_decode_v2
),
FINAL AS (
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
        symbol,
        product_id,
        digest,
        trader,
        subaccount,
        version,
        CASE
            WHEN amount < 0 THEN 'sell/short'
            WHEN amount > 0 THEN 'buy/long'
        END AS trade_type,
        expiration_raw,
        exp_binary,
        order_type AS order_type_raw,
        CASE
            WHEN order_type = 0 THEN 'default'
            WHEN order_type = 1 THEN 'immediate-or-cancel'
            WHEN order_type = 2 THEN 'fill-or-kill'
            WHEN order_type = 3 THEN 'post-only'
        END AS order_type,
        market_reduce_flag,
        expiration,
        nonce,
        CASE
            WHEN isTaker = 1 THEN TRUE
            WHEN isTaker = 0 THEN FALSE
        END AS is_taker,
        price_amount_unadj,
        price_amount,
        amount_unadj,
        amount,
        CASE
            WHEN quote_delta < 0 THEN quote_delta * -1
            ELSE quote_delta
        END AS amount_usd,
        fee_amount_unadj,
        fee_amount,
        base_delta_unadj AS base_delta_amount_unadj,
        base_delta AS base_delta_amount,
        quote_delta_unadj AS quote_delta_amount_unadj,
        quote_delta AS quote_delta_amount,
        _log_id,
        _inserted_timestamp
    FROM
        order_fill_format
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash','event_index']
    ) }} AS vertex_perps_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
