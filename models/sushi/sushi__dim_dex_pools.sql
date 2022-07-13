{{ config(
    materialized = 'table'
) }}

    SELECT
        lower(pair_contract_address) as pool_address,
        lower(token0_contract_address) as token0_address,
        token0_symbol||'-'||token1_symbol as pool_name,
        token0_symbol,
        token0_name,
        lower(token1_contract_address) as token1_address,
        token1_symbol,
        token1_name,
        token0_decimals,
        token1_decimals 
    FROM
         {{ source(
            'arbitrum_pools',
            'SUSHI_DIM_DEX_POOLS'
        ) }} 