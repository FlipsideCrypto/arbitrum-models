{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    tags = ['curated']
) }}

WITH api_pull AS (

    SELECT
        PARSE_JSON(
            live.udf_api(
                'https://api.gmx.io/tokens'
            )
        ) :data AS response
),
api_lateral_flatten AS (
    SELECT
        r.value :data AS VALUE
    FROM
        api_pull,
        LATERAL FLATTEN (response) AS r
),
product_metadata AS (
    SELECT
        LOWER(
            VALUE :address
        ) AS address,
        VALUE :decimals AS decimals,
        VALUE :symbol AS symbol
    FROM
        api_lateral_flatten
)
SELECT
    *
FROM
    product_metadata
