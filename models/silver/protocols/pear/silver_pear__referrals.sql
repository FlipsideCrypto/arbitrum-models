{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['referrer','referee'],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH log_pull AS (

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
        CONCAT(
            tx_hash,
            '-',
            event_index
        ) AS _log_id,
        modified_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        contract_address = '0x61126e4dcecbc7a43ac9fd783ccf66c62d9622fe'
        AND topic_0 IN (
            '0xd857c02cc639c01db6b14a3ff9f6c625012fdaa73829efa32719b3ebb4144968',
            '0x8729039de96215ec6db4a7775708511e52c141a25c89b9e69e97fca70c196d65'
        )
        AND tx_succeeded

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
),
referral_code_added AS (
    SELECT
        CONCAT('0x', SUBSTR(topic_1, 27, 40)) AS referrer,
        DATA :: STRING AS code
    FROM
        log_pull
    WHERE
        topic_0 = '0xd857c02cc639c01db6b14a3ff9f6c625012fdaa73829efa32719b3ebb4144968'
),
referee_added AS (
    SELECT
        block_timestamp,
        CONCAT('0x', SUBSTR(topic_1, 27, 40)) AS referee,
        DATA :: STRING AS code,
        _log_id,
        modified_timestamp
    FROM
        log_pull
    WHERE
        topic_0 = '0x8729039de96215ec6db4a7775708511e52c141a25c89b9e69e97fca70c196d65'
),
FINAL AS (
    SELECT
        block_timestamp,
        referee,
        referrer,
        arbitrum.utils.udf_hex_to_string(SUBSTRING(code, 3)) AS referral_code,
        _log_id
    FROM
        referral_code_added C
        LEFT JOIN referee_added r USING(code)
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(['referrer', 'referee']) }} AS referral_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{invocation_id}}' AS _invocation_id
FROM
    FINAL
