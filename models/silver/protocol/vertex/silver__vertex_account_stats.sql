{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

with trades_union as (
    select
*
from
    {{ ref('silver__vertex_perps') }}
union all
    select
    *
from
    {{ ref('silver__vertex_spot') }}
),
liquidations as (
    SELECT
        trader,
        subaccount,
        product_id,
        sum(amount) as total_liquidation_amount,
        sum(amount_quote) as total_liquidation_amount_quote,
        count(*) as liquidation_count
    FROM
        {{ ref('silver__vertex_liquidations') }}
    group by 1,2,3
)

select
    t.trader,
    t.subaccount,
    t.product_id,
    sum(amount) as total_volume_amount,
    sum(amount_usd) as total_usd_volume,
    count(*) as trade_count,
    sum(fee_amount) as total_fee_amount,
    sum(base_delta_amount) as total_base_delta_amount,
    sum(quote_delta_amount) as total_quote_delta_amount,
    sum(l.total_liquidation_amount) as total_liquidation_amount, 
    sum(l.total_liquidation_amount_quote) as total_liquidation_amount_quote,
    sum(liquidation_count) as total_liquidation_count
from
    trades_union t
LEFT JOIN
    liquidations l
ON
    t.trader = l.trader
AND
    t.subaccount = l.subaccount
AND
    t.product_id = l.product_id
group by 1,2,3;