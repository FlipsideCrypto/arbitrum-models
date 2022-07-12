{{ config(
    materialized = 'table'
) }}

    SELECT
        pair_contract_address as pool_address,
        token0_contract_address as token0_address,
        token0_symbol,
        token0_name,
        tokens1_contract_address as token1_address,
        token1_symbol,
        token0_name,
        token0_decimals,
        token1_decimals 
    FROM
         {{ source(
            'arbitrum_pools',
            'SUSHI_DIM_KASHI_POOLS'
        ) }} 