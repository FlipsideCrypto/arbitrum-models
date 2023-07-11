{{ config (
    materialized = "ephemeral"
) }}

SELECT
    DISTINCT COALESCE(
        tx.block_number,
        tr.block_number
    ) AS block_number
FROM
    {{ ref("silver__transactions") }}
<<<<<<< HEAD
    tx full
    OUTER JOIN {{ ref("silver__traces") }}
=======
    tx
    LEFT JOIN {{ ref("silver__traces") }}
>>>>>>> bc6332984b6fc8bb06e5876b6a1f9cf3ba3ed3b9
    tr
    ON tx.block_number = tr.block_number
    AND tx.tx_hash = tr.tx_hash
    AND tr.block_timestamp >= DATEADD(
        'day',
        -2,
        CURRENT_DATE
    )
WHERE
    tx.block_timestamp >= DATEADD(
        'day',
        -2,
        CURRENT_DATE
    )
    AND (
        tr.tx_hash IS NULL
        OR tx.tx_hash IS NULL
    )
