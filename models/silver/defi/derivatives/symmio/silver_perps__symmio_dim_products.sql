{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH logs AS (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        contract_address,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        event_index,
        topics,
        'AddSymbol' AS event_name,
        topic_0,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        TRY_TO_NUMBER(
            arbitrum.utils.udf_hex_to_int(
                segmented_data [0] :: STRING
            )
        ) AS symbol_id,
        arbitrum.utils.udf_hex_to_string(
            segmented_data [9] :: STRING
        ) AS NAME,
        TRY_TO_NUMBER(
            arbitrum.utils.udf_hex_to_int(
                segmented_data [2] :: STRING
            )
        ) * pow(
            10,
            -18
        ) AS min_acceptable_quote_value,
        TRY_TO_NUMBER(
            arbitrum.utils.udf_hex_to_int(
                segmented_data [3] :: STRING
            )
        ) * pow(
            10,
            -18
        ) AS min_acceptable_portion_lf,
        TRY_TO_NUMBER(
            arbitrum.utils.udf_hex_to_int(
                segmented_data [4] :: STRING
            )
        ) * pow(
            10,
            -18
        ) AS trading_fee,
        TRY_TO_NUMBER(
            arbitrum.utils.udf_hex_to_int(
                segmented_data [5] :: STRING
            )
        ) * pow(
            10,
            -18
        ) AS max_leverage,
        TRY_TO_NUMBER(
            arbitrum.utils.udf_hex_to_int(
                segmented_data [6] :: STRING
            )
        ) AS funding_rate_epoch_duration,
        TRY_TO_NUMBER(
            arbitrum.utils.udf_hex_to_int(
                segmented_data [7] :: STRING
            )
        ) AS funding_rate_window_time,
        CONCAT(
            tx_hash,
            '-',
            event_index
        ) AS _log_id,
        modified_timestamp as _inserted_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        contract_address = LOWER('0x8f06459f184553e5d04f07f868720bdacab39395')
        AND topic_0 = '0xf688a4e13cb28e5c12cf045718e469d8f189222cc94e8ffd4225f0703da46a72' --add_product
        AND tx_succeeded

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    event_index,
    topics,
    event_name,
    symbol_id AS product_id,
    topic_0,
    funding_rate_epoch_duration,
    funding_rate_window_time,
    max_leverage,
    min_acceptable_portion_lf,
    min_acceptable_quote_value,
    NAME AS product_name,
    trading_fee,
    _inserted_timestamp,
    _log_id,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash','product_id']
    ) }} AS symmio_products_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    logs qualify(ROW_NUMBER() over(PARTITION BY product_id
ORDER BY
    _inserted_timestamp DESC)) = 1
