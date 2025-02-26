{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH decoded_logs AS (

    SELECT
        *
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        topics [0] :: STRING IN ('0x468a25a7ba624ceea6e540ad6f49171b52495b648417ae91bca21676d8a24dc5','0x137a44067c8961cd7e1d876f4754a5a3a75989b4552f1843fc69c3b372def160')
        {# AND origin_to_address IN ('0x7c68c7866a64fa2160f78eeae12217ffbf871fa8','0x352f684ab9e97a6321a13cf03a61316b681d9fd2','0x9e0521c3dbb18e849f4955087e065e5c9c879917') #}

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
        _inserted_timestamp,
        decoded_flat:eventName :: STRING as event_name,
        decoded_flat:eventNameHash :: STRING as event_name_hash,
        decoded_flat:msgSender :: STRING as msg_sender,
        decoded_flat:topic1 :: STRING as topic_1,
        decoded_flat:topic2 :: STRING as topic_2,
        decoded_flat :eventData AS event_data
    FROM
        decoded_logs
)
SELECT
    *
FROM
    lat_flat
