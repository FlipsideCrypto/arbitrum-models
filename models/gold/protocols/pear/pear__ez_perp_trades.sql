{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'PEAR',
                'PURPOSE': 'PERPS, DEX, TRADES'
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
    trade_type,
    platform,
    symbol,
    market_type,
    is_taker,
    price_amount_unadj,
    price_amount,
    amount_unadj,
    amount,
    amount_usd,
    fee_amount_unadj,
    fee_amount,
    pear_perps_id as ez_perp_trades_id,
    inserted_timestamp,
    modified_timestamp
FROM 
    {{ ref('silver_pear__perps') }} 