{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['reorg','curated']
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
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS asset_address,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS borrow_address,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) :: INTEGER AS amount,
        utils.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) :: INTEGER AS collateral_only,
        p.token_name,
        p.token_symbol,
        p.token_decimals,
        l._log_id,
        l._inserted_timestamp
    FROM
        {{ ref('silver__logs') }} l 
    INNER JOIN
        {{ ref('silver__silo_pools') }} p  
    ON
        l.contract_address = p.silo_address
    WHERE
        topics [0] :: STRING = '0x312a5e5e1079f5dda4e95dbbd0b908b291fd5b992ef22073643ab691572c5b52'
        AND tx_status = 'SUCCESS' --excludes failed txs

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
    asset_address AS silo_market,
    amount / pow(
        10,
        token_decimals
    ) AS amount,
    collateral_only,
    LOWER(
        borrow_address
    ) AS borrow_address,
    'Silo' AS platform,
    token_name,
    token_symbol AS symbol,
    'ethereum' AS blockchain,
    _log_id,
    _inserted_timestamp
FROM
    deposits qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
