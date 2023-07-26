{{ config (
    materialized = 'view',
    tags = ['full_test']
) }}

SELECT
    *
FROM
    {{ ref('silver__transactions') }}
WHERE
    block_number NOT IN (
        SELECT
            block_number
        FROM
            {{ ref('silver_observability__excluded_receipt_blocks') }}
    )
    AND block_number > 22207815