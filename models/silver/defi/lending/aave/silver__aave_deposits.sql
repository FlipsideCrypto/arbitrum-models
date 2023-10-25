{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['non_realtime','reorg','curation']
) }}

WITH deposits AS(

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
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS reserve_1,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS onBehalfOf,
        utils.udf_hex_to_int(
            topics [3] :: STRING
        ) :: INTEGER AS refferal,
        CASE
            WHEN topics [0] :: STRING = '0xde6857219544bb5b7746f48ed30be6386fefc61b2f864cacf559893bf50fd951' THEN CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40))
            WHEN topics [0] :: STRING = '0xc12c57b1c73a2c3a2ea4613e9476abb3d8d146857aab7329e24243fb59710c82' THEN CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40))
            WHEN topics [0] :: STRING = '0x2b627736bca15cd5381dcf80b0bf11fd197d01a037c52b927a881a10fb73ba61' THEN CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 42))
        END AS userAddress,
        CASE
            WHEN topics [0] :: STRING = '0xde6857219544bb5b7746f48ed30be6386fefc61b2f864cacf559893bf50fd951' THEN utils.udf_hex_to_int(
                segmented_data [1] :: STRING
            ) :: INTEGER
            WHEN topics [0] :: STRING = '0xc12c57b1c73a2c3a2ea4613e9476abb3d8d146857aab7329e24243fb59710c82' THEN utils.udf_hex_to_int(
                segmented_data [0] :: STRING
            ) :: INTEGER
            WHEN topics [0] :: STRING = '0x2b627736bca15cd5381dcf80b0bf11fd197d01a037c52b927a881a10fb73ba61' THEN utils.udf_hex_to_int(
                segmented_data [1] :: STRING
            ) :: INTEGER
        END AS deposit_quantity,
        CASE
            WHEN contract_address = LOWER('0x7d2768de32b0b80b7a3454c06bdac94a69ddc7a9') THEN 'Aave V2'
            WHEN contract_address = LOWER('0x398eC7346DcD622eDc5ae82352F02bE94C62d119') THEN 'Aave V1'
            WHEN contract_address = LOWER('0x7937d4799803fbbe595ed57278bc4ca21f3bffcb') THEN 'Aave AMM'
            WHEN contract_address = LOWER('0x794a61358D6845594F94dc1DB02A252b5b4814aD') THEN 'Aave V3'
            ELSE 'ERROR'
        END AS aave_version,
        origin_from_address AS depositor_address,
        COALESCE(
            origin_to_address,
            contract_address
        ) AS lending_pool_contract,
        CASE
            WHEN reserve_1 = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            ELSE reserve_1
        END AS aave_market,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING = '0x2b627736bca15cd5381dcf80b0bf11fd197d01a037c52b927a881a10fb73ba61'

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
AND contract_address = LOWER('0x794a61358D6845594F94dc1DB02A252b5b4814aD') --v3
AND tx_status = 'SUCCESS' --excludes failed txs
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
        {{ ref('silver__aave_tokens') }}
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
    LOWER(
        aave_market
    ) AS aave_market,
    LOWER(
        atoken_meta.atoken_address
    ) AS aave_token,
    deposit_quantity / pow(
        10,
        atoken_meta.underlying_decimals
    ) AS issued_tokens,
    LOWER(
        depositor_address
    ) AS depositor_address,
    LOWER(
        lending_pool_contract
    ) AS lending_pool_contract,
    aave_version AS platform,
    atoken_meta.underlying_symbol AS symbol,
    'ethereum' AS blockchain,
    _log_id,
    _inserted_timestamp
FROM
    deposits
    LEFT JOIN atoken_meta
    ON deposits.aave_market = atoken_meta.underlying_address
    AND atoken_version = aave_version qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
