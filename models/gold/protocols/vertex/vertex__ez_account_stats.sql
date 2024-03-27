 {{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'VERTEX',
                'PURPOSE': 'CLOB, DEX, STATS'
            }
        }
    }
) }}

SELECT
    subaccount,
    trader,
    first_trade_timestamp,
    last_trade_timestamp,
    account_age,
    trade_count,
    perp_trade_count,
    spot_trade_count,
    long_count,
    short_count,
    total_usd_volume,
    total_fee_amount,
    total_base_delta_amount,
    total_quote_delta_amount,
    total_liquidation_amount,
    total_liquidation_amount_quote,
    total_liquidation_count,
    vertex_account_id,
    inserted_timestamp,
    modified_timestamp,
FROM
    {{ ref('silver__vertex_account_stats') }}