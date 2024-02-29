{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg'],
    enable=false
) }}


{% for item in range(1) %}
(
step_2 as (
    select
        response:ticker_id as ticker_id, 
        response:timestamp::STRING as timestamp,
        response:asks as asks,
        response:bids as bids
    from     
    (SELECT
        PARSE_JSON(
            live.udf_api(
                CONCAT('https://gateway.prod.vertexprotocol.com/v2/orderbook?ticker_id='ticker_id,'&depth=10000')
            )
        ) :data AS response
        FROM 
        (select
            ROW_NUMBER() OVER (ORDER BY product_id) AS row_num,
            product_id,
            ticker_id
        from
            {{ ref('silver__vertex_din_') }}}}
        where 
            product_id > 0
        order by 
            product_id
        WHERE row_num = {{ item + 1 }})
)
select 
    t.ticker_id,
    date_trunc('hour',
        try_to_timestamp(t.timestamp)
    ) as timestamp,
    'asks' as orderbook_side,
    a.value[0]::FLOAT as price,
    a.value[1]::FLOAT as volume 
from step_2 t,
LATERAL FLATTEN(input => t.asks) a
UNION ALL
select 
    t.ticker_id,
    date_trunc('hour',
        try_to_timestamp(t.timestamp)
    ) as timestamp,
    'bids' as orderbook_side,
    a.value[0]::FLOAT as price,
    a.value[1]::FLOAT as volume 
from step_2 t,
LATERAL FLATTEN(input => t.bids) a


) 
{% if not loop.last %}
UNION ALL
{% endif %}
{% endfor %}