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
    contract_address,
    symbol,
    type,
    price_increment_x18,
    size_increment,
    min_size,
    min_depth_x18,
    max_spread_rate_x18,
    maker_fee_rate_x18,
    taker_fee_rate_x18,
    long_weight_initial_x18,
    long_weight_maintenance_x18,
    COALESCE (
        vertex_product_id,
        {{ dbt_utils.generate_surrogate_key(
            ['product_id','contract_address']
        ) }}
    ) AS dim_products_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__vertex_dim_products') }}