{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'pool_address',
    tags = ['curated']
) }}

WITH pool_creation AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS token0,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS token1,
        CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS pool_address,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref ('core__fact_event_logs') }}
    WHERE
        contract_address IN (
            '0xac2ee06a14c52570ef3b9812ed240bce359772e7',
            --v2
            '0x9c2abd632771b433e5e7507bcaa41ca3b25d8544'
        ) --v3
        AND topics [0] :: STRING IN (
            '0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9',
            --PairCreated v2
            '0x91ccaa7a278130b65168c3a0c8d3bcae84cf5e43704342bd3ec0b59e59c036db'
        ) --v3
        AND tx_succeeded

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    contract_address,
    event_index,
    token0,
    token1,
    pool_address,
    _log_id,
    _inserted_timestamp
FROM
    pool_creation qualify(ROW_NUMBER() over (PARTITION BY pool_address
ORDER BY
    _inserted_timestamp DESC)) = 1
