{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'product_id',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH new_prod AS (

    SELECT
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        utils.udf_hex_to_int(
            's2c',
            segmented_data [0] :: STRING
        ) :: INT AS product_id,
        tx_hash,
        block_number,
        block_timestamp,
        _inserted_timestamp,
        _log_id
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING = '0x3286b0394bf1350245290b7226c92ed186bd716f28938e62dbb895298f018172'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND tx_hash NOT IN (
    SELECT
        tx_hash
    FROM
        {{ this }}
)
{% endif %}
),
book_address_pull AS (
    SELECT
        tx_hash,
        contract_address AS book_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        utils.udf_hex_to_int(
            's2c',
            segmented_data [0] :: STRING
        ) :: INT AS version
    FROM
        {{ ref('silver__logs') }}
    WHERE
        tx_hash IN (
            SELECT
                tx_hash
            FROM
                new_prod
        )
        AND topics [0] :: STRING = '0x7f26b83ff96e1f2b6a682f133852f6798a09c465da95921460cefb3847402498'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
),
api_pull AS (
    SELECT
        PARSE_JSON(
            live.udf_api(
                'https://gateway.sepolia-test.vertexprotocol.com/api/v2/assets'
            )
        ) :data AS response
),
api_lateral_flatten AS (
    SELECT
        r.value
    FROM
        api_pull,
        LATERAL FLATTEN (response) AS r
), 
product_metadata AS (
    SELECT
        VALUE :product_id AS product_id,
        VALUE :ticker_id AS ticker_id,
        VALUE :symbol AS symbol,
        VALUE :name AS NAME,
        VALUE :market_type AS market_type,
        VALUE :taker_fee AS taker_fee,
        VALUE :maker_fee AS maker_fee
    FROM
        api_lateral_flatten
),
final as (
    SELECT
        l.product_id,
        CASE
            WHEN l.product_id % 2 = 0 THEN 'perp'
            ELSE 'spot'
        END AS product_type,
        p.ticker_id,
        p.symbol,
        p.name,
        l.tx_hash,
        l.block_number,
        l.block_timestamp,
        C.book_address,
        C.version,
        _inserted_timestamp,
        _log_id
    FROM
        new_prod l
        LEFT JOIN book_address_pull C
        ON l.tx_hash = C.tx_hash
        LEFT JOIN product_metadata p
        ON l.product_id = p.product_id
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash','product_id']
    ) }} AS vertex_products_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL 
where product_id > 0 qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
