{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
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
        DATA,
        event_index,
        decoded_log,
        CONCAT(
            tx_hash,
            '-',
            event_index
        ) AS _log_id,
        CASE
            WHEN tx_status = 'SUCCESS' THEN TRUE
            ELSE FALSE
        END AS tx_succeeded,
        modified_timestamp
    FROM
        {{ ref('core__ez_decoded_event_logs') }}
    WHERE
        tx_succeeded
        AND contract_address = '0xc8ee91a54287db53897056e12d9819156d3822fb'

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(
            modified_timestamp
        ) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    topics,
    DATA,
    event_index,
    _log_id,
    modified_timestamp,
    decoded_log :eventName :: STRING AS event_name,
    decoded_log :eventNameHash :: STRING AS event_name_hash,
    decoded_log :msgSender :: STRING AS msg_sender,
    decoded_log :topic1 :: STRING AS topic_1,
    decoded_log :topic2 :: STRING AS topic_2,
    decoded_log :eventData AS event_data
FROM
    decoded_logs
