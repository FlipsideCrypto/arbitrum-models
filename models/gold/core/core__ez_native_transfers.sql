{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ['block_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(trace_address, from_address, to_address)",
    tags = ['non_realtime','reorg']
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    tx_position,
    trace_index,
    identifier, --deprecate
    trace_address, --new column
    TYPE, --new column
    from_address,
    to_address,
    amount,
    amount_precise_raw,
    amount_precise,
    amount_usd,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    COALESCE (
        native_transfers_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_hash', 'trace_index']
        ) }}
    ) AS ez_native_transfers_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
  
FROM
    {{ ref('silver__native_transfers') }}
