 {{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'COMPOUND, LODESTAR, AAVE, RADIANT, SILO',
                'PURPOSE': 'LENDING, LIQUIDATIONS'
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
    liquidator,
    borrower,
    protocol_market,
    collateral_token,
    collateral_token_symbol,
    amount_unadj,
    amount,
    amount_usd,
    debt_token,
    debt_token_symbol
FROM 
    {{ ref('silver__complete_lending_liquidations') }}