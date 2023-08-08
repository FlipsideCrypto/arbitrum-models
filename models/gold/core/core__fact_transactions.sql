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
    eth_value,
    eth_value_precise_raw,
    eth_value_precise,
    tx_fee,
    tx_fee_precise,
    gas_price_bid,
    gas_price_paid,
    gas_limit,
    gas_used,
    cumulative_gas_used,
    max_fee_per_gas,
    max_priority_fee_per_gas,
    input_data,
    status,
    r,
    s,
    v,
    tx_type,
    l1_block_number,
    gas_used_for_l1
FROM
    (
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
            tx_fee_precise,
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
            gas_used_for_l1,
            to_varchar(
                TO_NUMBER(REPLACE(DATA :value :: STRING, '0x'), REPEAT('X', LENGTH(REPLACE(DATA :value :: STRING, '0x'))))
            ) AS eth_value_precise_raw,
            IFF(LENGTH(eth_value_precise_raw) > 18, LEFT(eth_value_precise_raw, LENGTH(eth_value_precise_raw) - 18) || '.' || RIGHT(eth_value_precise_raw, 18), '0.' || LPAD(eth_value_precise_raw, 18, '0')) AS rough_conversion,
            IFF(
                POSITION(
                    '.000000000000000000' IN rough_conversion
                ) > 0,
                LEFT(rough_conversion, LENGTH(rough_conversion) - 19),
                REGEXP_REPLACE(
                    rough_conversion,
                    '0*$',
                    ''
                )
            ) AS eth_value_precise
        FROM
            {{ ref('silver__transactions') }}
    )
