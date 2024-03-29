 {{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'VERTEX',
                'PURPOSE': 'CLOB, DEX'
            }
        }
    }
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    edge_event_index,
    user_event_index,
    trader,
    symbol,
    edge_is_taker,
    user_is_taker,
    edge_trade_type,
    user_trade_type,
    edge_amount_usd,
    user_amount_usd,
    edge_quote_delta,
    user_quote_delta,
    edge_base_delta,
    user_base_delta,
    vertex_edge_trade_id,
    inserted_timestamp,
    modified_timestamp,
FROM
    {{ ref('silver__vertex_account_stats') }}