{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'book_address',
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
)
SELECT
    l.product_id,
    CASE
        WHEN product_id % 2 = 0 THEN 'perp'
        ELSE 'spot'
    END AS product_type,
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
WHERE
    product_id > 0
ORDER BY
    product_id
