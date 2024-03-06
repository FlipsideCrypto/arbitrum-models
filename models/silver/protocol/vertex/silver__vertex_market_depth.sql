{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['product_id','timestamp','price'],
    cluster_by = ['timestamp::DATE']
) }}

WITH market_depth AS ({% for item in range(55) %}

    SELECT
        t.ticker_id, 
        t.product_id, 
        DATE_TRUNC('hour', TRY_TO_TIMESTAMP(t.timestamp)) AS TIMESTAMP, 
        'asks' AS orderbook_side, 
        A.value [0] :: FLOAT AS price, 
        A.value [1] :: FLOAT AS volume, 
        SYSDATE() AS _inserted_timestamp
    FROM
        (
    SELECT
        response :ticker_id AS ticker_id, 
        response :timestamp :: STRING AS TIMESTAMP, 
        response :asks AS asks, 
        response :bids AS bids, 
        product_id
    FROM
        (
        SELECT
            PARSE_JSON(live.udf_api(CONCAT('https://gateway.prod.vertexprotocol.com/v2/orderbook?ticker_id=', ticker_id, '&depth=1000000'))) :data AS response, 
            product_id
        FROM
            (
            SELECT
                ROW_NUMBER() over (ORDER BY product_id) AS row_num, 
                product_id, 
                ticker_id
            FROM
                {{ ref('silver__vertex_dim_products') }}
            WHERE
                product_id > 0
            ORDER BY
                product_id)
            WHERE
                row_num = {{ item + 1 }})) t, LATERAL FLATTEN(input => t.asks) A
    UNION ALL
    SELECT
        t.ticker_id, 
        t.product_id, 
        DATE_TRUNC('hour', TRY_TO_TIMESTAMP(t.timestamp)) AS TIMESTAMP, 
        'bids' AS orderbook_side, 
        A.value [0] :: FLOAT AS price, 
        A.value [1] :: FLOAT AS volume, 
        SYSDATE() AS _inserted_timestamp
    FROM
        (
    SELECT
        response :ticker_id AS ticker_id, 
        response :timestamp :: STRING AS TIMESTAMP, 
        response :asks AS asks, 
        response :bids AS bids, 
        product_id
    FROM
        (
        SELECT
            PARSE_JSON(live.udf_api(CONCAT('https://gateway.prod.vertexprotocol.com/v2/orderbook?ticker_id=', ticker_id, '&depth=1000000'))) :data AS response, 
            product_id
        FROM
            (
            SELECT
                ROW_NUMBER() over (ORDER BY product_id) AS row_num, 
                product_id, 
                ticker_id
            FROM
                {{ ref('silver__vertex_dim_products') }}
            WHERE
                product_id > 0
            ORDER BY
                product_id)
            WHERE
                row_num = {{ item + 1 }})) t, LATERAL FLATTEN(input => t.bids) A 
        {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %})
SELECT
    *
FROM
    market_depth