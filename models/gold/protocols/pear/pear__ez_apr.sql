{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'PEAR',
                'PURPOSE': 'PERPS, DEX, APR'
            }
        }
    }
) }}

SELECT
    day,
    daily_fee_unadj,
    daily_fee,
    daily_fee_usd,
    fee_symbol,
    daily_fee_per_token_unadj,
    daily_fee_per_token,
    apr,
    pear_daily_apr_id as ez_apr_id,
    inserted_timestamp,
    modified_timestamp
FROM 
    {{ ref('silver__pear_apr') }} 