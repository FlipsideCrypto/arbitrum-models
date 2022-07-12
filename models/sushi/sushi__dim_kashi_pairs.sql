{{ config(
    materialized = 'table'
) }}

    SELECT
        pair_address, 
        pair_symbol as pair_name,
        asset_symbol,
        asset_address,
        collateral_symbol,
        collateral_address,
        asset_decimal as asset_decimals,
        collateral_decimal as collateral_decimals  
    FROM
         {{ source(
            'arbitrum_pools',
            'SUSHI_DIM_KASHI_POOLS'
        ) }} 