{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['tx_hash', 'quote_id'],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

with decoded_logs as (
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
        decoded_log:quoteId as quoteId,
        decoded_log:partyA as partyA,
        decoded_log:partyB as partyB,
        decoded_log:openedPrice*pow(10,-18) as openedPrice,
        decoded_log:filledAmount*pow(10,-18) as filledAmount,
        filledAmount*openedPrice as tx_vol_usd,
        modified_timestamp,
        ez_decoded_event_logs_id
    FROM
        arbitrum.core.ez_decoded_event_logs
    WHERE
        contract_address = LOWER('0x8f06459f184553e5d04f07f868720bdacab39395')
        AND topic_0 = '0xa50f98254710514f60327a4e909cd0be099a62f316299907ef997f3dc4d1cda5'
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
select 
    l.tx_hash,
    l.block_number,
    l.block_timestamp,
    l.event_name,
    l.origin_from_address,
    l.origin_to_address,
    l.topic_0,
    quoteId,
    partyA,
    partyB,
    openedPrice,
    filledAmount,
    tx_vol_usd,
    l.modified_timestamp as _inserted_timestamp,
    l.ez_decoded_event_logs_id,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'quoteId']
    ) }} AS pear_open_position_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    decoded_logs l