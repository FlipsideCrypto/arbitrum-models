{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'PEAR',
                'PURPOSE': 'PERPS, DEX, STATS'
            }
        }
    }
) }}

SELECT
    trader,
    first_trade_timestamp,
    last_trade_timestamp,
    account_age,
    trade_count,
    trade_count_mtd,
    gmx_trade_count,
    symmio_trade_count,
    vertex_trade_count,
    perp_trade_count,
    long_count,
    short_count,
    total_usd_volume,
    total_usd_volume_mtd,
    avg_usd_trade_size,
    total_fee_amount,
    total_liquidation_amount,
    total_liquidation_count,
    current_rebate_rate,
    current_rebate_tier,
    estimated_rebate_amount,
    most_recent_staked_amount,
    current_fee_discount_rate,
    pear_account_id as ez_account_stats_id,
    inserted_timestamp,
    modified_timestamp
FROM 
    {{ ref('silver__pear_account_stats') }} 