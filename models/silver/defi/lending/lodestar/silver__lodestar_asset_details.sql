{{ config(
    materialized = 'table',
    tags = ['static']
) }}
-- Pulls contract details for relevant c assets.  The case when handles cETH.
WITH asset_pull AS (

    SELECT
        block_number AS created_block,
        block_timestamp,
        tx_hash,
        token_name AS itoken_name,
        token_symbol AS itoken_symbol,
        token_decimals AS itoken_decimals,
        l.contract_address AS itoken_address,
        origin_from_address
    FROM
        {{ ref('silver__logs') }}
        l
        LEFT JOIN {{ ref('silver__contracts') }} C
        ON C.contract_address = l.contract_address
    WHERE
        topics [0] = '0x7ac369dbd14fa5ea3f473ed67cc9d598964a77501540ba6751eb0b3decf5870d'
        AND origin_from_address = '0x0f01756bc6183994d90773c8f22e3f44355ffa0e'
),
underlying_address_map AS(
    SELECT
        CASE
            WHEN A.itoken_address = '0xd12d43cdf498e377d3bfa2c6217f05b466e14228' THEN LOWER('0x17FC002b466eEc40DaE837Fc4bE5c67993ddBd6F') --FRAX
            WHEN A.itoken_address = '0x4987782da9a63bc3abace48648b15546d821c720' THEN LOWER('0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1') --ARB
            WHEN A.itoken_address = '0x9365181a7df82a1cc578eae443efd89f00dbb643' THEN LOWER('0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9') --ETH
            WHEN A.itoken_address = '0xfece754d92bd956f681a941cef4632ab65710495' THEN LOWER('0x0fBcbaEA96Ce0cF7Ee00A8c19c3ab6f5Dc8E1921') --DPX
            WHEN A.itoken_address = '0x8991d64fe388fa79a4f7aa7826e8da09f0c3c96a' THEN LOWER('0x912CE59144191C1204E64559FE8253a0e49E6548') --GMX TOKEN
            WHEN A.itoken_address = '0x2193c45244af12c280941281c8aa67dd08be0a64' THEN LOWER('0x82aF49447D8a07e3bd95BD0d56f35241523fBab1') --Plutus Vault GLP Token
            WHEN A.itoken_address = '0x5d27cff80df09f28534bb37d386d43aa60f88e25' THEN LOWER('0x6C2C06790b3E3E3c38e12Ee22F8183b37a13EE55') --MAGIC Token
            WHEN A.itoken_address = '0x79b6c5e1a7c0ad507e1db81ec7cf269062bab4eb' THEN LOWER('0xfc5A1A6EB076a2C7aD06eD22C90d7E710E35ad0a') --USDC
            WHEN A.itoken_address = '0xea0a73c17323d1a9457d722f10e7bab22dc0cb83' THEN LOWER('0x5326E71Ff593Ecc2CF7AcaE5Fe57582D6e74CFF1') --WBTC
            WHEN A.itoken_address = '0xf21ef887cb667f84b8ec5934c1713a7ade8c38cf' THEN LOWER('0xFEa7a6a0B346362BF88A9e4A88416B77a57D6c2A') --USDC.e
            WHEN A.itoken_address = '0x4c9aaed3b8c443b4b634d1a189a5e25c604768de' THEN LOWER('0xaf88d065e77c8cC2239327C5EDb3A432268e5831') --DAI
            WHEN A.itoken_address = '0xc37896bf3ee5a2c62cdbd674035069776f721668' THEN LOWER('0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f') --USDT
            WHEN A.itoken_address = '0x1ca530f02dd0487cef4943c674342c5aea08922f' THEN LOWER('0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8') --WSTETH
        END AS underlying_asset_address,
        C.token_name AS underlying_name,
        C.token_symbol AS underlying_symbol,
        C.token_decimals AS underlying_decimals,
        A.*
    FROM
        asset_pull A
        LEFT JOIN {{ ref('silver__contracts') }} C
        ON underlying_asset_address = C.contract_address
)
SELECT
    created_block,
    block_timestamp as create_timestamp,
    tx_hash as create_hash,
    itoken_name,
    itoken_symbol,
    itoken_decimals,
    itoken_address,
    underlying_name,
    underlying_symbol,
    underlying_decimals,
    underlying_asset_address
FROM
    underlying_address_map qualify(ROW_NUMBER() over(PARTITION BY itoken_address
ORDER BY
    created_block DESC)) = 1
