{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'lb_pair',
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
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS tokenX,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS tokenY,
        utils.udf_hex_to_int(
            topics [3] :: STRING
        ) :: INT AS binStep,
        CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS lb_pair,
        utils.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) :: INT AS pool_id,
        CASE
            WHEN contract_address = '0x1886d09c9ade0c5db822d85d21678db67b6c2982' THEN 'v2'
            WHEN contract_address = '0xb43120c4745967fa9b93e79c149e66b0f2d6fe0c' THEN 'v2.2'
            ELSE 'v2.1'
        END AS version,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        contract_address IN (
            '0x1886d09c9ade0c5db822d85d21678db67b6c2982',
            --v2
            '0x8e42f2f4101563bf679975178e880fd87d3efd4e',
            --v2.1
            '0xee0616a2deaa5331e2047bc61e0b588195a49cea',
            --v2.1
            '0x8597db3ba8de6baadeda8cba4dac653e24a0e57b',
            --v2.1
            '0xb43120c4745967fa9b93e79c149e66b0f2d6fe0c' --v2.2
        )
        AND topics [0] :: STRING = '0x2c8d104b27c6b7f4492017a6f5cf3803043688934ebcaa6a03540beeaf976aff' --LB PairCreated
        AND tx_status = 'SUCCESS'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    contract_address,
    event_index,
    tokenX,
    tokenY,
    binStep AS bin_step,
    lb_pair,
    pool_id,
    version,
    _log_id,
    _inserted_timestamp
FROM
    pool_creation qualify(ROW_NUMBER() over (PARTITION BY lb_pair
ORDER BY
    _inserted_timestamp DESC)) = 1
