{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg'],
    enable = false
) }}

WITH deposits AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        'Deposit' AS event_name,
        event_index,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topics [1], 27, 40)) :: STRING AS router,
        CONCAT('0x', SUBSTR(topics [2], 27, 40)) :: STRING AS reciever,
        utils.udf_hex_to_int(
            topics [3] :: STRING
        ) :: INT AS product_id,
        CONCAT('0x', SUBSTR(segmented_data [0], 25, 40)) :: STRING AS caller,
        CONCAT('0x', SUBSTR(segmented_data [1], 25, 40)) :: STRING AS token,
        utils.udf_hex_to_int(
            segmented_data [2] :: STRING
        ) :: INT AS amount,
        utils.udf_hex_to_int(
            segmented_data [3] :: STRING
        ) :: INT AS shares,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] = '0x5087c507ecc5d0c98a650e056183b51dc2354729f823139bbbdf3122f8f7bfa8'
        AND contract_address = '0x052ab3fd33cadf9d9f227254252da3f996431f75' --elixir vertex manager

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
token_join AS (
    SELECT
        d.block_number,
        d.block_timestamp,
        d.tx_hash,
        d.contract_address,
        event_name,
        event_index,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        router,
        reciever,
        d.product_id,
        A.symbol AS pool_symbol,
        caller,
        token AS token_address,
        token_symbol,
        amount AS amount_unadj,
        amount / pow(
            10,
            C.token_decimals
        ) AS amount,
        (
            amount / pow(
                10,
                C.token_decimals
            ) * p.price
        ) :: FLOAT AS amount_usd,
        d._log_id,
        d._inserted_timestamp
    FROM
        deposits d
        LEFT JOIN {{ ref('price__ez_hourly_token_prices') }}
        p
        ON d.token = p.token_address
        AND DATE_TRUNC(
            'hour',
            block_timestamp
        ) = p.hour
        LEFT JOIN {{ ref('silver__contracts') }} C
        ON d.token = C.contract_address
        LEFT JOIN {{ ref('silver__vertex_dim_products') }} A
        ON d.product_id = A.product_id
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash','event_index']
    ) }} AS vertex_deposit_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    token_join qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
