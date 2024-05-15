{{ config(
    materialized = 'incremental',
    full_refresh = false,
    unique_key = ['ticker_id','hour'],
    cluster_by = ['HOUR::DATE'],
    tags = 'curated'
) }}

WITH  market_stats_pull as (
    SELECT
        HOUR,
        ticker_id,
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
        base_volume_24h,
        quote_volume_24h,
        inserted_timestamp,
        vertex_market_stats_id,
        _invocation_id
    FROM
        {{ ref('silver__vertex_market_stats') }}
{% if is_incremental() %}
WHERE inserted_timestamp >= (
    SELECT
        MAX(
            inserted_timestamp
        )
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    *,
    SYSDATE() AS modified_timestamp
FROM
    market_stats_pull qualify(ROW_NUMBER() over(PARTITION BY ticker_id, HOUR
ORDER BY
    inserted_timestamp DESC NULLS LAST)) = 1