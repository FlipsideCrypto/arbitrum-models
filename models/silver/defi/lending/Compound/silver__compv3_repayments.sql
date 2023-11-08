{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['non_realtime','reorg']
) }}

WITH repayments AS (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        contract_address AS asset,
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
        'ethereum' AS blockchain,
        _log_id,
        l._inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
        l
        LEFT JOIN {{ ref('silver__compv3_asset_details') }} C
        ON contract_address = C.compound_market_address
    WHERE
        topics [0] = '0xd1cf3d156d5f8f0d50f6c122ed609cec09d35c9b9fb3fff6ea0959134dae424e' --Supply
        AND contract_address IN (
            '0xa17581a9e3356d9a858b789d68b4d866e593ae94',
            '0xc3d688b66703497daa19211eedff47f25384cdc3'
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
    w.asset as compound_market,
    repayer,
    borrower,
    depositor,
    underlying_asset AS repay_asset,
    amount / pow(
        10,
        w.compound_market_decimals
    ) AS repayed_tokens,
    w.underlying_asset_symbol AS repay_asset_symbol,
    compound_version,
    blockchain,
    _log_id,
    _inserted_timestamp
FROM
    repayments w qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
