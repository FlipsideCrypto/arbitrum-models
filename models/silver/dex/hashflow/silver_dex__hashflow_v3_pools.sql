{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    tags = ['non_realtime']
) }}

WITH contract_deployments AS (

    select
        tx_hash,
        block_number,
        block_timestamp,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        origin_from_address as deployer_address,
        CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS pool_address,
        _log_id,
        _inserted_timestamp
    from 
        {{ ref('silver__logs') }} 
    where 
        contract_address = lower('0xdE828fdc3F497F16416D1bB645261C7C6a62DAb5')
    AND
        TOPICS[0]::string = '0xdbd2a1ea6808362e6adbec4db4969cbc11e3b0b28fb6c74cb342defaaf1daada'

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
    tx_hash,
    block_number,
    block_timestamp,
    deployer_address,
    pool_address,
    _log_id,
    _inserted_timestamp
FROM 
    contract_deployments
