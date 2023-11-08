{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['non_realtime','reorg']
) }}

WITH borrow AS (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        contract_address AS asset,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS src_address,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS to_address,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) :: INTEGER AS borrow_amount,
        origin_from_address AS borrower_address,
        'Compound V3' AS compound_version,
        C.compound_market_name AS NAME,
        C.compound_market_symbol AS symbol,
        C.compound_market_decimals AS decimals,
        C.underlying_asset_address,
        C.underlying_asset_symbol,
        'ethereum' AS blockchain,
        _log_id,
        l._inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
        l
        LEFT JOIN {{ ref('silver__compv3_asset_details') }} C
        ON asset = C.compound_market_address
    WHERE
        topics [0] = '0x9b1bfa7fa9ee420a16e124f794c35ac9f90472acc99140eb2f6447c714cad8eb' --withdrawl
        AND l.contract_address IN (
            LOWER('0xA5EDBDD9646f8dFF606d7448e414884C7d905dCA'),
            LOWER('0x9c4ec768c28520B50860ea7a15bd7213a9fF58bf')
        )

{% if is_incremental() %}
AND l._inserted_timestamp >= (
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
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    w.asset AS compound_market,
    borrower_address,
    w.underlying_asset_address AS token_address,
    borrow_amount / pow(
        10,
        w.decimals
    ) AS borrowed_tokens,
    w.symbol as ctoken_symbol,
    compound_version,
    w.underlying_asset_symbol AS token_symbol,
    blockchain,
    _log_id,
    _inserted_timestamp
FROM
    borrow w qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
