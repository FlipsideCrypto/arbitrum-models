{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'subaccount',
    tags = 'curated'
) }}

with 
{% if is_incremental() %}
new_subaccount_actions as (
    SELECT
        distinct(subaccount)
    FROM
        {{ ref('silver__vertex_perps') }}
    WHERE
_inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
    UNION
    SELECT
        distinct(subaccount)
    FROM
        {{ ref('silver__vertex_perps') }}
    WHERE
_inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
    UNION
    SELECT
        distinct(subaccount)
    FROM
        {{ ref('silver__vertex_liquidations') }}
    WHERE
_inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) - INTERVAL '12 hours'
    FROM
        {{ this }}
)

),
{% endif %}
trades_union as (
    select
        subaccount,
        trader,
        digest,
        'perp' as product_type,
        trade_type,
        block_timestamp,
        amount,
        amount_usd,
        fee_amount,
        base_delta_amount,
        quote_delta_amount,
        _inserted_timestamp
    from
        {{ ref('silver__vertex_perps') }}
{% if is_incremental() %}
WHERE subaccount in (select subaccount from new_subaccount_actions)
{% endif %}
    union all
    select
        subaccount,
        trader,
        digest,
        'spot' as product_type,
        trade_type,
        block_timestamp,
        amount,
        amount_usd,
        fee_amount,
        base_delta_amount,
        quote_delta_amount,
        _inserted_timestamp
    from
        {{ ref('silver__vertex_spot') }}
{% if is_incremental() %}
WHERE subaccount in (select subaccount from new_subaccount_actions)
{% endif %}
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
{% if is_incremental() %}
WHERE subaccount in (select subaccount from new_subaccount_actions)
{% endif %}
    group by 1,2
),
fianl as (
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
        count(distinct(digest)) as trade_count,
        sum(case
            when product_type = 'perp' THEN +1
            else 0 end)
        as perp_trade_count,
        sum(case
            when product_type = 'spot' THEN +1
            else 0 end) as spot_trade_count,
        sum(case
            when trade_type = 'buy/long' THEN +1
            else 0 end) as long_count,
        sum(case
            when trade_type = 'sell/short' THEN +1
            else 0 end) as short_count,
        sum(amount_usd) as total_usd_volume,
        sum(fee_amount) as total_fee_amount,
        sum(base_delta_amount) as total_base_delta_amount,
        sum(quote_delta_amount) as total_quote_delta_amount,
        max(l.total_liquidation_amount) as total_liquidation_amount, 
        max(l.total_liquidation_amount_quote) as total_liquidation_amount_quote,
        max(liquidation_count) as total_liquidation_count,
        MAX(t._inserted_timestamp) as _inserted_timestamp
    from
        trades_union t
    LEFT JOIN
        liquidations l
    ON
        t.trader = l.trader
    AND
        t.subaccount = l.subaccount
    group by 1,2
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['subaccount','trader']
    ) }} AS vertex_account_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
