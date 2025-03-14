{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['referrer','referee'],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH referral_code_added AS (

    SELECT
        block_timestamp,
        tx_hash,
        contract_address,
        event_index,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        topics,
        topic_0,
        topic_1,
        DATA,
        CONCAT('0x', SUBSTR(topic_1, 27, 40)) AS referrer,
        DATA :: STRING AS code,
        CONCAT(
            tx_hash,
            '-',
            event_index
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        contract_address = '0x61126e4dcecbc7a43ac9fd783ccf66c62d9622fe'
        AND topic_0 = '0xd857c02cc639c01db6b14a3ff9f6c625012fdaa73829efa32719b3ebb4144968'

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
referee_added AS (
    SELECT
        block_timestamp,
        tx_hash,
        contract_address,
        event_index,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        topics,
        topic_0,
        topic_1,
        DATA,
        CONCAT('0x', SUBSTR(topic_1, 27, 40)) AS referee,
        DATA :: STRING AS code,
        CONCAT(
            tx_hash,
            '-',
            event_index
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        contract_address = '0x61126e4dcecbc7a43ac9fd783ccf66c62d9622fe'
        AND topic_0 = '0x8729039de96215ec6db4a7775708511e52c141a25c89b9e69e97fca70c196d65'

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
FINAL AS (
    SELECT
        r.block_timestamp,
        r.tx_hash,
        r.contract_address,
        r.event_index,
        r.origin_function_signature,
        r.origin_from_address,
        r.origin_to_address,
        r.topics,
        referee,
        referrer,
        arbitrum.utils.udf_hex_to_string(SUBSTRING(code, 3)) AS referral_code,
        r._log_id,
        r._inserted_timestamp
    FROM
        referral_code_added C
        RIGHT JOIN referee_added r USING(code)
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(['referrer', 'referee']) }} AS referral_id,
    _inserted_timestamp,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{invocation_id}}' AS _invocation_id
FROM
    FINAL
