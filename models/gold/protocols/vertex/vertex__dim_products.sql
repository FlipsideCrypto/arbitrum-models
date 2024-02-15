 {{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'VERTEX',
                'PURPOSE': 'DEX, SPOT'
            }
        }
    }
) }}

SELECT
    product_id,
    product_type,
    ticker_id,
    symbol,
    name,
    tx_hash,
    block_number,
    block_timestamp,
    book_address,
    version,
    _inserted_timestamp,
    _log_id
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__vertex_dim_products') }}