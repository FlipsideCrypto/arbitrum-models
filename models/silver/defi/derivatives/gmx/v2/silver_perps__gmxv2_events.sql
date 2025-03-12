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
        topic_0,
        topic_1,
        topic_2,
        event_index,
        decoded_log :eventName :: STRING AS event_name,
        decoded_log :eventNameHash :: STRING AS event_name_hash,
        decoded_log :msgSender :: STRING AS msg_sender,
        decoded_log :eventData AS event_data,
        CONCAT(
            tx_hash,
            '-',
            event_index
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__ez_decoded_event_logs') }}
    WHERE
        topic_0 :: STRING IN (
            '0x468a25a7ba624ceea6e540ad6f49171b52495b648417ae91bca21676d8a24dc5',
            '0x137a44067c8961cd7e1d876f4754a5a3a75989b4552f1843fc69c3b372def160'
        )

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
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    topic_0,
    topic_1,
    topic_2,
    event_index,
    event_name,
    event_name_hash,
    msg_sender,
    event_data,
    _log_id,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['_log_id']
    ) }} AS gmxv2_events_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    decoded_logs
WHERE
    event_name IN (
        'OrderCreated',
        'OrderExecuted',
        'PositionDecrease'
    )
