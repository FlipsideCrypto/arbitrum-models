{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'product_id',
    cluster_by = ['timestamp::DATE']
) }}

WITH 
market_depth as (
{% for item in range(75) %}
    select 
        t.ticker_id,
        t.product_id,
        date_trunc('hour',
            try_to_timestamp(t.timestamp)
        ) as timestamp,
        'asks' as orderbook_side,
        a.value[0]::FLOAT as price,
        a.value[1]::FLOAT as volume,
        SYSDATE() AS _inserted_timestamp
    from (select
        response:ticker_id as ticker_id, 
        response:timestamp::STRING as timestamp,
        response:asks as asks,
        response:bids as bids,
        product_id
    from     
    (SELECT
        PARSE_JSON(
            live.udf_api(
                CONCAT('https://gateway.prod.vertexprotocol.com/v2/orderbook?ticker_id=',ticker_id,'&depth=1000000')
            )
        ) :data AS response,
        product_id
        FROM 
        (select
            ROW_NUMBER() OVER (ORDER BY product_id) AS row_num,
            product_id,
            ticker_id
        from
            {{ ref('silver__vertex_dim_products') }}
        where 
            product_id > 0
        order by 
            product_id
        )
        WHERE row_num = {{ item + 1 }})) t,
    LATERAL FLATTEN(input => t.asks) a
    UNION ALL
    select 
        t.ticker_id,
        t.product_id,
        date_trunc('hour',
            try_to_timestamp(t.timestamp)
        ) as timestamp,
        'bids' as orderbook_side,
        a.value[0]::FLOAT as price,
        a.value[1]::FLOAT as volume,
        SYSDATE() AS _inserted_timestamp
    from (select
        response:ticker_id as ticker_id, 
        response:timestamp::STRING as timestamp,
        response:asks as asks,
        response:bids as bids,
        product_id
    from     
    (SELECT
        PARSE_JSON(
            live.udf_api(
                CONCAT('https://gateway.prod.vertexprotocol.com/v2/orderbook?ticker_id=',ticker_id,'&depth=1000000')
            )
        ) :data AS response,
        product_id
        FROM 
        (select
            ROW_NUMBER() OVER (ORDER BY product_id) AS row_num,
            product_id,
            ticker_id
        from
            {{ ref('silver__vertex_dim_products') }}
        where 
            product_id > 0
        order by 
            product_id
        )
        WHERE row_num = {{ item + 1 }})) t,
    LATERAL FLATTEN(input => t.bids) a

{% if not loop.last %}
UNION ALL
{% endif %}
{% endfor %}
),
market_depth_format as (
    SELECT
        timestamp as hour,
        ticker_id,
        product_id,
        orderbook_side,
        min(price) as min_price,
        max(price) as max_price,
        median(price) as median_price,
        avg(price) as avg_price,
        sum(volume) as volume,
        _inserted_timestamp
    from
        market_depth
    GROUP BY 1,2,3,4,10
),
