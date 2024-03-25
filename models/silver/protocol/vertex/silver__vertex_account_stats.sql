{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'subaccount',
    tags = ['curated','reorg']
) }}

with trades_union as (
    select
        subaccount,
        trader,
        digest,
        block_timestamp,
        amount,
        amount_usd,
        fee_amount,
        base_delta_amount,
        quote_delta_amount
    from
        {{ ref('silver__vertex_perps') }}
    union all
    select
        subaccount,
        trader,
        digest,
        block_timestamp,
        amount,
        amount_usd,
        fee_amount,
        base_delta_amount,
        quote_delta_amount
    from
        {{ ref('silver__vertex_spot') }}
),
liquidations as (
    SELECT
        trader,
        subaccount,
        sum(amount) as total_liquidation_amount,
        sum(amount_quote) as total_liquidation_amount_quote,
        count(*) as liquidation_count
    FROM
        {{ ref('silver__vertex_liquidations') }}
    group by 1,2
)

select
    t.subaccount,
    t.trader,
    min(t.block_timestamp) as first_trade_timestamp,
    max(t.block_timestamp) as last_trade_timestamp,
    DATEDIFF(
        'day',
        first_trade_timestamp,    
        last_trade_timestamp
    ) AS account_age,
    sum(amount) as total_volume_amount,
    sum(amount_usd) as total_usd_volume,
    count(distinct(digest)) as trade_count,
    sum(fee_amount) as total_fee_amount,
    sum(base_delta_amount) as total_base_delta_amount,
    sum(quote_delta_amount) as total_quote_delta_amount,
    max(l.total_liquidation_amount) as total_liquidation_amount, 
    max(l.total_liquidation_amount_quote) as total_liquidation_amount_quote,
    max(liquidation_count) as total_liquidation_count,
    SYSDATE() as _inserted_timestamp
from
    trades_union t
LEFT JOIN
    liquidations l
ON
    t.trader = l.trader
AND
    t.subaccount = l.subaccount
group by 1,2
