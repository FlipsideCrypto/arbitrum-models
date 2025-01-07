{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH service_multisigs AS (

    SELECT
        DISTINCT multisig_address,
        id
    FROM
        {{ ref('silver_olas__create_service_multisigs') }}
)
SELECT
    d.block_number,
    d.block_timestamp,
    d.tx_hash,
    d.origin_function_signature,
    d.origin_from_address,
    d.origin_to_address,
    d.contract_address,
    d.event_index,
    s.multisig_address,
    s.id AS service_id,
    d.topic_0,
    d.topic_1,
    d.topic_2,
    d.topic_3,
    d.data,
    regexp_substr_all(SUBSTR(d.data, 3, len(d.data)), '.{64}') AS segmented_data,
    CONCAT(
        d.tx_hash,
        '-',
        d.event_index :: STRING
    ) AS _log_id,
    d.modified_timestamp AS _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['d.tx_hash','d.event_index']
    ) }} AS service_event_logs_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('core__fact_event_logs') }}
    d
    INNER JOIN service_multisigs s
    ON d.origin_to_address = s.multisig_address
WHERE
    d.tx_succeeded

{% if is_incremental() %}
AND d.modified_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
