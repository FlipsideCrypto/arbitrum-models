{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['reorg','curated']
) }}

WITH repayments AS (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        l.contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        l.contract_address AS asset,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS repayer,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS borrower,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) :: INTEGER AS amount,
        utils.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) :: INTEGER AS usd_value,
        origin_from_address AS depositor,
        'Compound V3' AS compound_version,
        compound_market_name,
        compound_market_symbol,
        compound_market_decimals,
        C.underlying_asset_address AS underlying_asset,
        C.underlying_asset_symbol,
        'arbitrum' AS blockchain,
        _log_id,
        l._inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
        l
        LEFT JOIN {{ ref('silver__comp_asset_details') }} C
        ON contract_address = C.compound_market_address
    WHERE
        topics [0] = '0xd1cf3d156d5f8f0d50f6c122ed609cec09d35c9b9fb3fff6ea0959134dae424e' --Supply
        AND l.contract_address IN (
            LOWER('0xA5EDBDD9646f8dFF606d7448e414884C7d905dCA'),
            LOWER('0x9c4ec768c28520B50860ea7a15bd7213a9fF58bf')
        )
        AND tx_status = 'SUCCESS'

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
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    w.asset AS compound_market,
    repayer,
    borrower,
    depositor,
    underlying_asset AS token_address,
    w.underlying_asset_symbol AS token_symbol,
    amount AS amount_unadj,
    amount / pow(
        10,
        w.compound_market_decimals
    ) AS amount,
    compound_version,
    blockchain,
    _log_id,
    _inserted_timestamp
FROM
    repayments w
WHERE
    compound_market IN (
        '0xa5edbdd9646f8dff606d7448e414884c7d905dca',
        '0x9c4ec768c28520b50860ea7a15bd7213a9ff58bf'
    ) qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
