{{ config(
    materialized = 'incremental',
    unique_key = ['ticker_id','hour'],
    cluster_by = ['HOUR::DATE']
) }}

WITH api_pull AS (

    SELECT
        PARSE_JSON(
            live.udf_api(
                'https://archive.prod.vertexprotocol.com/v2/contracts'
            )
        ) :data AS response
),
market_stats AS (
    SELECT
        DATE_TRUNC('hour', SYSDATE()) AS HOUR,
        f.value :base_currency :: STRING AS base_currency,
        f.value :base_volume :: FLOAT AS base_volume,
        f.value :contract_price :: FLOAT AS contract_price,
        f.value :contract_price_currency :: STRING AS contract_price_currency,
        f.value :funding_rate :: FLOAT AS funding_rate,
        f.value :index_price :: FLOAT AS index_price,
        f.value :last_price :: FLOAT AS last_price,
        f.value :mark_price :: FLOAT AS mark_price,
        TRY_TO_TIMESTAMP(
            f.value :next_funding_rate_timestamp :: STRING
        ) AS next_funding_rate_timestamp,
        f.value :open_interest :: FLOAT AS open_interest,
        f.value :open_interest_usd :: FLOAT AS open_interest_usd,
        f.value :price_change_percent_24h :: FLOAT AS price_change_percent_24h,
        f.value :product_type :: STRING AS product_type,
        f.value :quote_currency :: STRING AS quote_currency,
        f.value :quote_volume :: FLOAT AS quote_volume,
        f.key AS ticker_id,
        SYSDATE() AS _inserted_timestamp
    FROM
        api_pull A,
        LATERAL FLATTEN(
            input => response
        ) AS f
),
trade_snapshot as (
    
    select
        DATE_TRUNC('hour', SYSDATE()) AS HOUR,
        concat(symbol,'_USDC') as ticker_id,
        symbol,
        product_id,
        count(distinct(tx_hash)) as distinct_sequencer_batches, --may need to change this or just delete
        count(distinct(trader)) as distinct_trader_count,
        count(distinct(subaccount)) as distinct_subaccount_count,
        count(*) as trade_count,
        sum(amount_usd) as amount_usd,
        sum(fee_amount) as fee_amount,
        sum(base_delta_amount) as base_delta_amount,
        sum(quote_delta_amount) as quote_delta_amount
    from
        {{ ref('silver__vertex_perps') }} p 
    where 
    1=1
{% if is_incremental() %}
AND block_timestamp > (select MAX(inserted_timestamp) from {{this}})
{% endif %}
    group by 
        1,2,3,4
UNION ALL
    select
        DATE_TRUNC('hour', SYSDATE()) AS HOUR,
        concat(symbol,'_USDC') as ticker_id,
        symbol,
        product_id,
        count(distinct(tx_hash)) as distinct_sequencer_batches,
        count(distinct(trader)) as distinct_trader_count,
        count(distinct(subaccount)) as distinct_subaccount_count,
        count(*) as trade_count,
        sum(amount_usd) as amount_usd,
        sum(fee_amount) as fee_amount,
        sum(base_delta_amount) as base_delta_amount,
        sum(quote_delta_amount) as quote_delta_amount
    from
        {{ ref('silver__vertex_spot') }}
    where 
    1=1
{% if is_incremental() %}
AND block_timestamp > (select MAX(inserted_timestamp) from {{this}})
{% endif %}
    group by 
        1,2,3,4
 
),
FINAL AS (
    SELECT
        s.hour,
        s.ticker_id,
        t.symbol,
        t.product_id,
        t.distinct_sequencer_batches,
        t.distinct_trader_count,
        t.distinct_subaccount_count,
        t.trade_count,
        t.amount_usd,
        t.fee_amount,
        t.base_delta_amount,
        t.quote_delta_amount,
        s.base_volume as base_volume_24h,
        s.quote_volume as quote_volume_24h,
        s.contract_price,
        s.contract_price_currency,
        s.funding_rate,
        s.index_price,
        s.last_price,
        s.mark_price,
        s.next_funding_rate_timestamp,
        s.open_interest,
        s.open_interest_usd,
        s.price_change_percent_24h,
        s.product_type,
        s.quote_currency,
        s.quote_volume,
        SYSDATE() AS inserted_timestamp,
        SYSDATE() AS modified_timestamp
    FROM
        market_stats s
    LEFT JOIN
        trade_snapshot t
    ON
        t.ticker_id = s.ticker_id
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['ticker_id','inserted_timestamp']
    ) }} AS vertex_market_stats_id,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL qualify(ROW_NUMBER() over(PARTITION BY ticker_id
ORDER BY
    HOUR DESC)) = 1
