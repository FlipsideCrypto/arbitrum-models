{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    block_number,
    block_timestamp,
    block_hash,
    tx_hash,
    nonce,
    POSITION,
    origin_function_signature,
    from_address,
    to_address,
    VALUE AS eth_value,
    tx_fee,
    gas_price AS gas_price_bid,
    effective_gas_price AS gas_price_paid,
    gas AS gas_limit,
    gas_used,
    cumulative_gas_used,
    max_fee_per_gas,
    max_priority_fee_per_gas,
    input_data,
    tx_status AS status,
    r,
    s,
    v,
    tx_type,
    l1_block_number,
    gas_used_for_l1
FROM
    {{ ref('silver__transactions') }}
