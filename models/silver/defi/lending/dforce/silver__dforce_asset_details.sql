{{ config(
    materialized = 'table',
    tags = ['static']
) }}

WITH log_pull AS (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        contract_address
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING = '0x70aea8d848e8a90fb7661b227dc522eb6395c3dac71b63cb59edd5c9899b2364'
    AND 
        origin_from_address = LOWER('0x70A0D319c76B0a99BE5e8cd2685219aeA9406845')
),
contracts AS (
    SELECT
        *
    FROM
        {{ ref('silver__contracts') }}
),
contract_pull AS (
    SELECT
        l.tx_hash,
        l.block_number,
        l.block_timestamp,
        l.contract_address,
        C.token_name,
        C.token_symbol,
        C.token_decimals,
        CASE
            WHEN l.contract_address = '0x0385f851060c09a552f1a28ea3f612660256cbaa' THEN '0x641441c631e2f909700d2f41fd87f0aa6a6b4edb'
            WHEN l.contract_address = '0xaea8e2e7c97c5b7cd545d3b152f669bae29c4a63' THEN '0xae6aab43c4f3e0cea4ab83752c278f8debaba689'
            WHEN l.contract_address = '0x013ee4934ecbfa5723933c4b08ea5e47449802c8' THEN '0xf97f4df75117a78c1a5a0dbb814af92458539fb4'
            WHEN l.contract_address = '0x8dc3312c68125a94916d62b97bb5d925f84d4ae0' THEN '0xff970a61a04b1ca14834a43f5de4533ebddb5cc8'
            WHEN l.contract_address = '0xf6995955e4b0e5b287693c221f456951d612b628' THEN '0xda10009cbd5d07dd0cecc66161fc93d7c9000da1'
            WHEN l.contract_address = '0x46eca1482fffb61934c4abca62abeb0b12feb17a' THEN '0xfa7f8980b0f1e64a2062791cc3b0871572f1f7f0'
            WHEN l.contract_address = '0xf52f079af080c9fb5afca57dde0f8b83d49692a9' THEN '0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9'
            WHEN l.contract_address = '0xd3204e4189becd9cd957046a8e4a643437ee0acc' THEN '0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f'
            WHEN l.contract_address = '0x5675546eb94c2c256e6d7c3f7dcab59bea3b0b8b' THEN '0xc2125882318d04d266720b598d620f28222f3abd'
            WHEN l.contract_address = '0xf5854ae761db79d7f794e429fc3d8102565cad61' THEN '0x82af49447d8a07e3bd95bd0d56f35241523fbab1'
            ELSE NULL
        END AS underlying_asset
    FROM
        log_pull l
        LEFT JOIN contracts C
        ON C.contract_address = l.contract_address qualify(ROW_NUMBER() over(PARTITION BY l.contract_address
    ORDER BY
        block_timestamp ASC)) = 1
)
SELECT
    l.tx_hash,
    l.block_number,
    l.block_timestamp,
    l.contract_address AS token_address,
    l.token_name,
    l.token_symbol,
    l.token_decimals,
    l.underlying_asset AS underlying_asset_address,
    C.token_name AS underlying_name,
    C.token_symbol AS underlying_symbol,
    C.token_decimals AS underlying_decimals
FROM
    contract_pull l
    LEFT JOIN contracts C
    ON C.contract_address = l.underlying_asset
WHERE
    underlying_asset IS NOT NULL
