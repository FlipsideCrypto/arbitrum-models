{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

select
    block_number,
    block_timestamp,
    tx_hash,
    contract_address,
    'UpdateDecreaseOrder' AS event_name,
    event_index,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
    utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) :: INT AS order_index,
    CONCAT('0x', SUBSTR(segmented_data [1] :: STRING, 25, 40)) AS collateral_token,
    utils.udf_hex_to_int(
            segmented_data [2] :: STRING
        ) :: INT AS collateral_delta,
    CONCAT('0x', SUBSTR(segmented_data [3] :: STRING, 25, 40)) AS index_token,
    utils.udf_hex_to_int(
            segmented_data [4] :: STRING
        ) :: FLOAT AS size_delta,
    utils.udf_hex_to_int(
            segmented_data [5] :: STRING
        ) :: INT AS is_long,
    utils.udf_hex_to_int(
            segmented_data [6] :: STRING
        ) :: FLOAT AS trigger_price,
    utils.udf_hex_to_int(
            segmented_data [7] :: STRING
        ) :: INT AS trigger_above_threshold,
        _log_id,
        _inserted_timestamp
from 
    {{ ref('silver__logs') }}
where 
    topics[0]::string = '0x75781255bc71c83f89f29e5a2599f2c174a562d2cd8f2e818a47f132e7280498'