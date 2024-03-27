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
    hour,
    ticker_id,
    product_id,
    symbol,
    distinct_sequencer_batches,
    distinct_trader_count,
    distinct_subaccount_count,
    trade_count,
    amount_usd,
    fee_amount,
    base_delta_amount,
    quote_delta_amount,
    base_currency,
    base_volume,
    contract_price,
    contract_price_currency,
    funding_rate,
    index_price,
    last_price,
    mark_price,
    next_funding_rate_timestamp,
    open_interest,
    open_interest_usd,
    price_change_percent_24h,
    product_type,
    quote_currency,
    quote_volume,
    vertex_market_stats_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__vertex_market_stats') }}