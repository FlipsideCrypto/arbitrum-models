{{ config (
    materialized = 'view',
    tags = ['full_test']
) }}

SELECT
    *
FROM
    {{ ref('silver__confirmed_blocks') }}
WHERE block_number > 22207814
