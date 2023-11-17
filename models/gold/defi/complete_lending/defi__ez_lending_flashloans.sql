 {{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'COMPOUND, LODESTAR, AAVE, RADIANT, SILO',
                'PURPOSE': 'LENDING, FLASHLOANS'
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
    platform,
    initiator,
    target,
    protocol_market,
    flashloan_token,
    flashloan_token_symbol,
    amount_unadj,
    flashloan_amount,
    flashloan_amount_usd
FROM 
    {{ ref('silver__complete_lending_flashloans') }}