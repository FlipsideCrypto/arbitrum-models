{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    tx_position,
    trace_index,
    '0x' AS trace_address,
    -- new
    '0x' AS TYPE,
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
    ) AS modified_timestamp,
    concat_ws(
        '-',
        block_number,
        tx_position,
        CONCAT(
            TYPE,
            '_',
            trace_address
        )
    ) AS _call_id,
    -- deprecate
    CONCAT(
        TYPE,
        '_',
        trace_address
    ) AS identifier --deprecate
FROM
    {{ ref('silver__native_transfers') }}
