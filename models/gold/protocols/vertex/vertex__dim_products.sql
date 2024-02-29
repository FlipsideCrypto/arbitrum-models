 {{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'VERTEX',
                'PURPOSE': 'CLOB, DEX, PRODUCTS'
            }
        }
    }
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    product_id,
    book_address,
    product_type,
    ticker_id,
    symbol,
    name,
    version,
    COALESCE (
        vertex_products_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_hash', 'product_id']
        ) }}
    ) AS dim_products_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__vertex_dim_products') }}
ORDER BY product_id