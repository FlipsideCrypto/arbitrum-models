{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH
api_pull AS (
    SELECT
        PARSE_JSON(
            live.udf_api(
                'https://api.gmx.io/tokens'
            )
        ) :data  AS response
),
api_lateral_flatten AS (
    SELECT
        r.value:data as value
    FROM
        api_pull,
        LATERAL FLATTEN (response) AS r
)
,
product_metadata AS (
    SELECT
        lower(VALUE :address) AS address,
        VALUE :decimals AS decimals,
        VALUE :symbol AS symbol
    FROM
        api_lateral_flatten
)
select * from product_metadata