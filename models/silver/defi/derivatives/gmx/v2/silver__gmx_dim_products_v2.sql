{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH api_pull AS (
    SELECT
        PARSE_JSON(
            live.udf_api(
                'https://arbitrum-api.gmxinfra.io/tokens'
            )
        ) :data :tokens AS response
),
api_lateral_flatten AS (
    SELECT
        r.value AS VALUE
    FROM
        api_pull,
        LATERAL FLATTEN (response) AS r
),
product_metadata AS (
    SELECT
        LOWER(
            VALUE :address
        ) AS address,
        VALUE :decimals:: INT AS decimals,
        VALUE :symbol:: STRING AS symbol,
        VALUE :synthetic:: STRING AS synthetic
    FROM
        api_lateral_flatten
),
market_pull AS (
    SELECT
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS market_address,
        CONCAT(
            '0x',
            SUBSTR(
                segmented_data [24] :: STRING,
                25,
                40
            )
        ) AS index_token,
        CONCAT(
            '0x',
            SUBSTR(
                segmented_data [28] :: STRING,
                25,
                40
            )
        ) AS long_token,
        CONCAT(
            '0x',
            SUBSTR(
                segmented_data [32] :: STRING,
                25,
                40
            )
        ) AS short_token,
        *
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING = '0x137a44067c8961cd7e1d876f4754a5a3a75989b4552f1843fc69c3b372def160'
        AND origin_function_signature = '0xa50ff3a6' --CreateMarket
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    CASE 
        WHEN index_token = '0x0000000000000000000000000000000000000000' THEN p2.address
        ELSE P.address
    END AS address,
    CASE 
        WHEN index_token = '0x0000000000000000000000000000000000000000' THEN p2.decimals
        ELSE P.decimals
    END AS decimals,
    CASE 
        WHEN index_token = '0x0000000000000000000000000000000000000000' THEN p2.symbol
        ELSE P.symbol
    END AS symbol,
    CASE 
        WHEN index_token = '0x0000000000000000000000000000000000000000' THEN p2.synthetic
        ELSE P.synthetic
    END AS synthetic,
    index_token,
    long_token,
    short_token,
    market_address,
    tx_status,
    contract_address,
    block_hash,
    data,
    event_index,
    event_removed,
    topics,
    _inserted_timestamp,
    _log_id,
    is_pending
FROM
    market_pull m
    LEFT JOIN product_metadata p
    ON m.index_token = p.address
    LEFT JOIN product_metadata p2
    ON m.short_token = p2.address
