{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver','defi','lending','curated']
) }}

WITH flashloan AS (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS target_address,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS initiator_address,
        CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS radiant_market,
        utils.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) :: INTEGER AS flashloan_quantity,
        utils.udf_hex_to_int(
            segmented_data [3] :: STRING
        ) :: INTEGER AS premium_quantity,
        utils.udf_hex_to_int(
            segmented_data [3] :: STRING
        ) :: INTEGER AS refferalCode,
        COALESCE(
            origin_to_address,
            contract_address
        ) AS lending_pool_contract,
        modified_timestamp AS _inserted_timestamp,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        topics [0] :: STRING = '0x631042c832b07452973831137f2d73e395028b44b250dedc5abb0ee766e168ac'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'

{% endif %}
AND contract_address IN (
    LOWER('0x2032b9A8e9F7e76768CA9271003d3e43E1616B1F'),
    LOWER('0xF4B1486DD74D07706052A33d31d7c0AAFD0659E1')
)
AND tx_succeeded
),
atoken_meta AS (
    SELECT
        atoken_address,
        atoken_symbol,
        atoken_name,
        atoken_decimals,
        underlying_address,
        underlying_symbol,
        underlying_name,
        underlying_decimals,
        atoken_version,
        atoken_created_block,
        atoken_stable_debt_address,
        atoken_variable_debt_address
    FROM
        {{ ref('silver__radiant_tokens') }}
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
    radiant_market,
    atoken_meta.atoken_address AS radiant_token,
    flashloan_quantity AS flashloan_amount_unadj,
    flashloan_quantity / pow(
        10,
        atoken_meta.underlying_decimals
    ) AS flashloan_amount,
    premium_quantity AS premium_amount_unadj,
    premium_quantity / pow(
        10,
        atoken_meta.underlying_decimals
    ) AS premium_amount,
    initiator_address AS initiator_address,
    target_address AS target_address,
    CASE
        WHEN contract_address = LOWER('0x2032b9A8e9F7e76768CA9271003d3e43E1616B1F') THEN 'Radiant V1'
        WHEN contract_address = LOWER('0xF4B1486DD74D07706052A33d31d7c0AAFD0659E1') THEN 'Radiant V2'
    END AS platform,
    atoken_meta.underlying_symbol AS symbol,
    'arbitrum' AS blockchain,
    _log_id,
    _inserted_timestamp
FROM
    flashloan
    LEFT JOIN atoken_meta
    ON flashloan.radiant_market = atoken_meta.underlying_address qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
