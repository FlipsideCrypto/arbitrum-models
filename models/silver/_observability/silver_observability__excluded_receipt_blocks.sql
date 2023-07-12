{{ config(
    materialized = 'view'
) }}

SELECT
    column1 AS block_number
FROM
    (
        VALUES
            (4527955),
            (15458950)
    ) AS block_number(column1)
