{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'pool_address',
    tags = ['curated']
) }}

WITH pools AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS baseToken,
        CONCAT('0x', SUBSTR(segmented_data [1] :: STRING, 25, 40)) AS quoteToken,
        CONCAT('0x', SUBSTR(segmented_data [2] :: STRING, 25, 40)) AS creator,
        CONCAT('0x', SUBSTR(segmented_data [3] :: STRING, 25, 40)) AS pool_address,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        contract_address IN (
            '0xa6cf3d163358af376ec5e8b7cc5e102a05fde63d',
            '0xddb13e6dd168e1a68dc2285cb212078ae10394a9',
            --dpp - factory,
            '0x7b07164ecfaf0f0d85dfc062bc205a4674c75aa0',
            '0x1506b54a1c0ea1b2f4a84866ec5776f7f6e7f0b1',
            --dpp oracle
            '0x9340e3296121507318874ce9c04afb4492af0284',
            '0xc8fe2440744dcd733246a4db14093664defd5a53',
            --dsp - factory
            '0xda4c4411c55b0785e501332354a036c04833b72b' --dvm - factory
        )
        AND topics [0] :: STRING IN (
            '0x8494fe594cd5087021d4b11758a2bbc7be28a430e94f2b268d668e5991ed3b8a',
            --NewDPP
            '0xbc1083a2c1c5ef31e13fb436953d22b47880cf7db279c2c5666b16083afd6b9d',
            --NewDSP
            '0xaf5c5f12a80fc937520df6fcaed66262a4cc775e0f3fceaf7a7cfe476d9a751d' --NewDVM
        )
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
    event_index,
    contract_address,
    baseToken AS base_token,
    quoteToken AS quote_token,
    creator,
    pool_address,
    _log_id,
    _inserted_timestamp
FROM
    pools qualify(ROW_NUMBER() over (PARTITION BY pool_address
ORDER BY
    _inserted_timestamp DESC)) = 1
