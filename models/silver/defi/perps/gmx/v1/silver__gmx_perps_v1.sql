{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

with log_pull as (
    select
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        'ExecutveIncreasePosition' AS event_name,
        event_index,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        CONCAT('0x', SUBSTR(topics[1] :: STRING, 27, 40)) AS account,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(segmented_data [11] :: STRING, 25, 40)) AS path_1,
        CONCAT('0x', SUBSTR(segmented_data [12] :: STRING, 25, 40)) AS path_2,
        CONCAT('0x', SUBSTR(segmented_data [1] :: STRING, 25, 40)) AS index_token,
        utils.udf_hex_to_int(
                segmented_data [2] :: STRING
            ) :: INT AS amount_in,
        utils.udf_hex_to_int(
                segmented_data [3] :: STRING
            ) :: INT AS min_out,
        utils.udf_hex_to_int(
                segmented_data [4] :: STRING
            ) :: STRING AS size_delta,
        utils.udf_hex_to_int(
                segmented_data [5] :: STRING
            ) :: INT AS is_long,
        utils.udf_hex_to_int(
                segmented_data [6] :: STRING
            ) :: STRING AS acceptable_price,
        utils.udf_hex_to_int(
                segmented_data [7] :: STRING
            ) :: INT AS execution_price,
            _log_id,
            _inserted_timestamp
    from 
        {{ ref('silver__logs') }}
    where 
        topics[0]::string = '0x1be316b94d38c07bd41cdb4913772d0a0a82802786a2f8b657b6e85dbcdfc641' --exec inc position
)
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
    account,
    path_1,
    path_2,
    index_token,
    amount_in,
    min_out,
    size_delta as size_delta_unadj,
    size_delta / pow(
        10,
        30
    ) as size_delta,
    is_long,
    acceptable_price,
    execution_price,
    _log_id,
    _inserted_timestamp
FROM
    log_pull