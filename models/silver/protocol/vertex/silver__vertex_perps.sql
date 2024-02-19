{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'digest',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH order_fill_decode AS (

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
        {{ ref('silver__logs') }}
        l
        INNER JOIN {{ ref('silver__vertex_dim_products') }}
        s
        ON s.book_address = l.contract_address
    WHERE
        topics [0] :: STRING = '0x224253ad5cda2459ff587f559a41374ab9243acbd2daff8c13f05473db79d14c'
        AND s.product_type = 'perp'

{% if is_incremental() %}
AND l._inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
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
        digest,
        trader,
        subaccount,
        expiration AS expiration_raw,
        TRY_TO_TIMESTAMP(expiration) AS expiration,
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
),

FINAL AS  (
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
        digest,
        trader,
        subaccount,
        CASE
            WHEN amount < 0 THEN 'sell/short'
            WHEN amount > 0 THEN 'buy/long'
        END AS trade_type,
        expiration_raw,
        expiration,
        nonce,
        CASE
            WHEN isTaker = 1 THEN TRUE
            WHEN isTaker = 0 THEN FALSE
        END AS isTaker,
        price_amount_unadj,
        price_amount,
        amount_unadj,
        amount,
        CASE
            WHEN trade_type = 'sell/short' THEN CAST(
                base_delta * price_amount AS FLOAT
            ) * -1
            WHEN trade_type = 'buy/long' THEN CAST(
                base_delta * price_amount AS FLOAT
            )
        END AS amount_usd,
        fee_amount_unadj,
        fee_amount,
        base_delta_unadj,
        base_delta,
        quote_delta_unadj,
        quote_delta,
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
