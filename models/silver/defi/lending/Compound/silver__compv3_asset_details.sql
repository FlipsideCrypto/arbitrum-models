{{ config(
    materialized = 'view'
) }}

SELECT
    lower('0xA5EDBDD9646f8dFF606d7448e414884C7d905dCA') AS compound_market_address,
    'Compound USDC (Bridged)' AS compound_market_name,
    'cUSDCv3' AS compound_market_symbol,
    6 AS compound_market_decimals,
    lower('0xaf88d065e77c8cC2239327C5EDb3A432268e5831') AS underlying_asset_address,
    'USDC' AS underlying_asset_name,
    'USDC' AS underlying_asset_symbol,
    6 AS underlying_asset_decimals
UNION ALL
SELECT
    lower('0x9c4ec768c28520B50860ea7a15bd7213a9fF58bf') AS compound_market_address,
    'Compound USDC' AS compound_market_name,
    'cUSDCv3' AS compound_market_symbol,
    6 AS compound_market_decimals,
    lower('0xaf88d065e77c8cC2239327C5EDb3A432268e5831') AS underlying_asset_address,
    'USDC' AS underlying_asset_name,
    'USDC' AS underlying_asset_symbol,
    6 AS underlying_asset_decimals
