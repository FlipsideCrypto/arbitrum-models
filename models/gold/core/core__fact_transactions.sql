{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    from_address,
    to_address,
    origin_function_signature,
    VALUE,
    value_precise_raw,
    value_precise,
    tx_fee,
    tx_fee_precise,
    CASE
        WHEN tx_status = 'SUCCESS' THEN TRUE
        ELSE FALSE
    END AS tx_succeeded,
    -- new column
    tx_type,
    nonce,
    POSITION AS tx_position,
    -- new
    gas_price AS gas_price_bid,
    effective_gas_price AS gas_price_paid,
    gas AS gas_limit,
    gas_used,
    cumulative_gas_used,
    max_fee_per_gas,
    max_priority_fee_per_gas,
    input_data,
    r,
    s,
    v,
    l1_block_number,
    gas_used_for_l1,
    COALESCE (
        transactions_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_hash']
        ) }}
    ) AS fact_transactions_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp,
    block_hash,
    -- deprecate
    tx_status AS status -- deprecate
FROM
    {{ ref('silver__transactions') }}
