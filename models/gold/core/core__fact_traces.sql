{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    1 AS tx_position,
    -- new column
    trace_index,
    from_address,
    to_address,
    input,
    output,
    TYPE,
    '0x' AS trace_address,
    --new column
    sub_traces,
    DATA,
    VALUE,
    value_precise_raw,
    value_precise,
    '0x' AS value_hex,
    --new column
    gas,
    gas_used,
    '0x' AS origin_from_address,
    -- new column
    '0x' AS origin_to_address,
    -- new column
    '0x' AS origin_function_signature,
    -- new column
    TRUE AS trace_succeeded,
    -- new column
    error_reason,
    '0x' AS revert_reason,
    -- new column
    TRUE AS tx_succeeded,
    -- new column
    before_evm_transfers,
    after_evm_transfers,
    fact_traces_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp,
    identifier,
    -- deprecate
    tx_status,
    -- deprecate
    trace_status -- deprecate
FROM
    {{ ref('silver__fact_traces2') }}
