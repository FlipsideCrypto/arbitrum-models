{% test vertex_missing_products(
    model,
    filter) %}

with recent_records as (
    select * from  {{model}}
    where modified_timestamp >= SYSDATE() - INTERVAL '12 hours'
),

invalid_product_ids as (
    select distinct product_id
    from {{ ref('silver__vertex_dim_products') }}
    where product_id not in (select product_id from recent_records)
    {% if filter %}
        AND {{ filter }}
    {% endif %}
)

select * 
from invalid_product_ids

{% endtest %}