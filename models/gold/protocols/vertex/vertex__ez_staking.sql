 {{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'VERTEX',
                'PURPOSE': 'CLOB, DEX, STAKING'
            }
        }
    }
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    function_name,
    symbol,
    amount_unadj,
    amount,
    amount_usd,
    vertex_staking_id,
    inserted_timestamp,
    modified_timestamp,
    vertex_staking_id
FROM
    {{ ref('silver__vertex_staking') }}