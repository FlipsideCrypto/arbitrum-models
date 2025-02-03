{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['reorg','curated']
) }}

WITH withdraw AS (

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
        l.contract_address AS compound_market,
        CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS token_address,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) :: INTEGER AS withdraw_amount,
        origin_from_address AS depositor_address,
        'Compound V3' AS compound_version,
        C.token_name,
        C.token_symbol,
        C.token_decimals,
        'arbitrum' AS blockchain,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        l.modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
        l
        LEFT JOIN {{ ref('silver__contracts') }} C
        ON token_address = C.contract_address
    WHERE
        topics [0] = '0xd6d480d5b3068db003533b170d67561494d72e3bf9fa40a266471351ebba9e16' --WithdrawCollateral
        AND tx_succeeded

{% if is_incremental() %}
AND l.modified_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND l.modified_timestamp >= SYSDATE() - INTERVAL '7 day'
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
    compound_market,
    depositor_address,
    w.token_address,
    w.token_symbol,
    withdraw_amount AS amount_unadj,
    withdraw_amount / pow(
        10,
        w.token_decimals
    ) AS amount,
    compound_version,
    blockchain,
    _log_id,
    _inserted_timestamp
FROM
    withdraw w
WHERE
    compound_market IN (
        '0xa5edbdd9646f8dff606d7448e414884c7d905dca',
        '0x9c4ec768c28520b50860ea7a15bd7213a9ff58bf'
    ) qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
