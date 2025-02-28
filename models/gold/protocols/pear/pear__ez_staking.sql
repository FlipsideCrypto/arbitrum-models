{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'PEAR',
                'PURPOSE': 'PERPS, DEX, STAKING'
            }
        }
    }
) }}

SELECT
    block_timestamp,
    tx_hash,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    action,
    user_address,
    amount,
    reward_token,
    pear_staking_id as ez_staking_id,
    inserted_timestamp,
    modified_timestamp
FROM 
    {{ ref('silver__pear_staking') }} 