{% test vertex_missing_products_market_stats(model) %}

with recent_records as (
    select * from  {{model}}
    where modified_timestamp >= SYSDATE() - INTERVAL '12 hours'
),

invalid_product_ids as (
    select distinct product_id
    from {{ ref('silver__vertex_dim_products') }}
    where product_id not in (select product_id from recent_records)
    AND product_type = 'perp'
    AND product_id <> 0
)

select * 
from invalid_product_ids

{% endtest %}

{% test vertex_missing_products_market_depth(model) %}

with recent_records as (
    select * from  {{model}}
    where modified_timestamp >= SYSDATE() - INTERVAL '12 hours'
),

invalid_product_ids as (
    select distinct product_id
    from {{ ref('silver__vertex_dim_products') }}
    where product_id not in (select product_id from recent_records)
    AND product_id <> 0 and (product_type = 'perp' and taker_fee is NULL)
)

select * 
from invalid_product_ids

{% endtest %}
