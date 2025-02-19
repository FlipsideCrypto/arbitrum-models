{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['tx_hash', 'quote_id'],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH decoded_logs AS (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        contract_address,
        event_name,
        decoded_log,
        topic_0,
        decoded_log :quoteId AS quote_id,
        decoded_log :closeId AS close_id,
        decoded_log :partyA AS partyA,
        decoded_log :partyB AS partyB,
        decoded_log :quoteStatus AS quoteStatus,
        decoded_log :closedPrice * pow(
            10,
            -18
        ) AS closedPrice,
        decoded_log :filledAmount * pow(
            10,
            -18
        ) AS filledAmount,
        filledAmount * closedPrice AS tx_vol_usd,
        modified_timestamp,
        ez_decoded_event_logs_id
    FROM
        arbitrum.core.ez_decoded_event_logs
    WHERE
        contract_address = LOWER('0x8f06459f184553e5d04f07f868720bdacab39395')
        AND topic_0 = '0xfa7483d69b899cf16df47cc736ab853f88135f704980d7d358a9746aead7a321' -- fill close position
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
)
SELECT
    l.tx_hash,
    l.block_number,
    l.block_timestamp,
    l.event_name,
    l.origin_from_address,
    l.origin_to_address,
    l.topic_0,
    quote_id,
    close_id,
    partyA,
    partyB,
    quoteStatus,
    closedPrice,
    filledAmount,
    tx_vol_usd,
    l.modified_timestamp AS _inserted_timestamp,
    l.ez_decoded_event_logs_id,
    {{ dbt_utils.generate_surrogate_key(
        ['l.tx_hash', 'l.quote_id']
    ) }} AS pear_perps_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    decoded_logs l
