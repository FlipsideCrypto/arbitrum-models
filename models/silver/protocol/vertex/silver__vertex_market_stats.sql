{{ config(
    materialized = 'incremental',
    unique_key = ['ticker_id','hour'],
    cluster_by = ['HOUR::DATE']
) }}

WITH api_pull AS (

    SELECT
        PARSE_JSON(
            live.udf_api(
                'https://archive.prod.vertexprotocol.com/v2/contracts'
            )
        ) :data AS response
),
market_stats as (
    SELECT
        DATE_TRUNC('hour', SYSDATE()) AS HOUR,
        f.value :base_currency :: STRING AS base_currency,
        f.value :base_volume :: FLOAT AS base_volume,
        f.value :contract_price :: FLOAT AS contract_price,
        f.value :contract_price_currency :: STRING AS contract_price_currency,
        f.value :funding_rate :: FLOAT AS funding_rate,
        f.value :index_price :: FLOAT AS index_price,
        f.value :last_price :: FLOAT AS last_price,
        f.value :mark_price :: FLOAT AS mark_price,
        TRY_TO_TIMESTAMP(
            f.value :next_funding_rate_timestamp :: STRING
        ) AS next_funding_rate_timestamp,
        f.value :open_interest :: FLOAT AS open_interest,
        f.value :open_interest_usd :: FLOAT AS open_interest_usd,
        f.value :price_change_percent_24h :: FLOAT AS price_change_percent_24h,
        f.value :product_type :: STRING AS product_type,
        f.value :quote_currency :: STRING AS quote_currency,
        f.value :quote_volume :: FLOAT AS quote_volume,
        f.key AS ticker_id,
        SYSDATE() AS _inserted_timestamp
    FROM
        api_pull A,
        LATERAL FLATTEN(
            input => response
        ) AS f
),
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
FINAL AS (
    SELECT
        s.HOUR,
        s.base_currency,
        s.base_volume,
        s.contract_price,
        s.contract_price_currency,
        s.funding_rate,
        s.index_price,
        s.last_price,
        s.mark_price,
        s.next_funding_rate_timestamp,
        s.open_interest,
        s.open_interest_usd,
        s.price_change_percent_24h,
        s.product_type,
        s.quote_currency,
        s.quote_volume,
        s.ticker_id,
        d.product_id,
        d.orderbook_side,
        d.min_price,
        d.max_price,
        d.median_price,
        d.avg_price,
        d.volume
    FROM
        market_stats s
    LEFT JOIN
        market_depth_format d
    ON
        s.ticker_id = d.ticker_id
    AND
        s.hour = d.hour
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['ticker_id','hour']
    ) }} AS vertex_market_stats_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL qualify(ROW_NUMBER() over(PARTITION BY ticker_id
ORDER BY
    HOUR DESC)) = 1