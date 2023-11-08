{{ config(
    materialized = 'incremental',
    tags = ['curated']
) }}

WITH silo_pull AS (

    SELECT
        block_number AS silo_create_block,
        l.contract_address AS factory_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS silo_address,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS token_address,
        utils.udf_hex_to_int(
            SUBSTR(
                segmented_data [0] :: STRING,
                27,
                40
            )
        ) :: INTEGER AS version,
        l._inserted_timestamp,
        l._log_id
    FROM
        {{ ref('silver__logs') }}
        l
    WHERE
        l.contract_address = LOWER('0x4166487056A922D784b073d4d928a516B074b719')

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
)
SELECT
    silo_create_block,
    factory_address,
    silo_address,
    l.token_address,
    version,
    c.token_name,
    c.token_symbol,
    c.token_decimals,
    l._log_id,
    l._inserted_timestamp
FROM
    silo_pull l
    LEFT JOIN {{ ref('silver__contracts') }} C
    ON C.contract_address = l.token_address
