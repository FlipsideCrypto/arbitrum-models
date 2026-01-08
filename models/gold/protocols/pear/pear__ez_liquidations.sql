{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'PEAR',
                'PURPOSE': 'PERPS, DEX, LIQUIDATIONS'
            }
        }
    }
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    contract_address,
    event_name,
    event_index,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    trader,
    amount,
    amount_usd,
    pear_liquidations_id as ez_liquidations_id,
    inserted_timestamp,
    modified_timestamp
FROM 
    {{ ref('silver_pear__liquidations') }} 