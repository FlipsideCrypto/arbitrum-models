{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['reorg','curated']
) }}

WITH liquidations AS(

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
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS depositor_address,
        origin_from_address AS receiver_address,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) :: INTEGER AS ShareAmountRepaid,
        utils.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) :: INTEGER AS amount,
        p.token_address as silo_market,
        l._log_id,
        l._inserted_timestamp
    FROM
        {{ ref('silver__logs') }} l 
    INNER JOIN
        {{ ref('silver__silo_pools') }} p  
    ON
        l.contract_address = p.silo_address
    WHERE
        topics [0] :: STRING = '0xf3fa0eaee8f258c23b013654df25d1527f98a5c7ccd5e951dd77caca400ef972'
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
    d.contract_address,
    silo_market,
    depositor_address,
    receiver_address,
    asset_address AS token_address,
    c.token_symbol,
    token_decimals,
    amount AS amount_unadj,
    amount / pow(
        10,
        c.token_decimals
    ) AS amount,
    'Silo' AS platform,
    'arbitrum' AS blockchain,
    d._log_id,
    d._inserted_timestamp
FROM
    liquidations d
LEFT JOIN
    {{ ref('silver__contracts') }} c
ON
    d.asset_address = c.contract_address qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    d._inserted_timestamp DESC)) = 1
