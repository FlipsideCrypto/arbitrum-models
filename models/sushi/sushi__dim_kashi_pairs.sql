{{ config(
    materialized = 'table',
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'SUSHI',
                'PURPOSE': 'DEFI, DEX'
            }
        }
    }
) }}

    SELECT
        lower(pair_address) as pair_address, 
        pair_symbol as pair_name,
        asset_symbol,
        lower(asset_address) as asset_address,
        collateral_symbol,
        lower(collateral_address) as collateral_address,
        asset_decimal as asset_decimals,
        collateral_decimal as collateral_decimals  
    FROM
         {{ source(
            'arbitrum_pools',
            'SUSHI_DIM_KASHI_POOLS'
        ) }} 