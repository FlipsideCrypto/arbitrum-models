{{ config(
    materialized = 'view',
    tags = ['observability']
) }}

SELECT 
    15458950 AS block_number, 
    '0xf135954c7b2a17c094f917fff69aa215fa9af86443e55f167e701e39afa5ff0f' AS tx_hash
UNION ALL
SELECT
    4527955,
    '0x1d76d3d13e9f8cc713d484b0de58edd279c4c62e46e963899aec28eb648b5800'