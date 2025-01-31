{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH decoded_logs AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        topics,
        event_index,
        decoded_flat,
        CONCAT( tx_hash, '-', event_index ) AS _log_id,
        CASE 
            WHEN tx_status = 'SUCCESS' THEN TRUE 
            ELSE FALSE 
        END AS tx_succeeded, 
        modified_timestamp 
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        1=1
        AND tx_succeeded
        AND contract_address='0xc8ee91a54287db53897056e12d9819156d3822fb'

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(
            modified_timestamp
        ) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
),

lat_flat AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        topics,
        event_index,
        _log_id,
        modified_timestamp,
        decoded_flat:eventName :: STRING as event_name,
        decoded_flat:eventNameHash :: STRING as event_name_hash,
        decoded_flat:msgSender :: STRING as msg_sender,
        decoded_flat:topic1 :: STRING as topic_1,
        decoded_flat:topic2 :: STRING as topic_2,
        decoded_flat:eventData AS event_data
    FROM
        decoded_logs
)
SELECT
    *
FROM
    lat_flat