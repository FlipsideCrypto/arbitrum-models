{{ config (
    materialized = "ephemeral"
) }}

WITH lookback AS (

    SELECT
        block_number
    FROM
        {{ ref("_block_lookback") }}
)
SELECT
    DISTINCT tx.block_number block_number
FROM
    {{ ref("silver__transactions") }}
    tx
    LEFT JOIN {{ ref("core__fact_traces") }}
    tr
    ON tx.block_number = tr.block_number
    AND tx.tx_hash = tr.tx_hash
WHERE
    tx.block_timestamp >= DATEADD('hour', -84, SYSDATE())
    AND tr.tx_hash IS NULL
    AND tx.to_address <> '0x000000000000000000000000000000000000006e'
    AND tx.block_number >= (
        SELECT
            block_number
        FROM
            lookback
    )
    AND tr.block_timestamp >= DATEADD('hour', -84, SYSDATE())
    AND tr.block_timestamp IS NOT NULL
