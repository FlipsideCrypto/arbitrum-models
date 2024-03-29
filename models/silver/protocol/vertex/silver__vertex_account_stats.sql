{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'subaccount',
    tags = 'curated'
) }}

WITH

{% if is_incremental() %}
new_subaccount_actions AS (

    SELECT
        DISTINCT(subaccount)
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
        DISTINCT(subaccount)
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
        DISTINCT(subaccount)
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

trades_union AS (
    SELECT
        subaccount,
        trader,
        digest,
        'perp' AS product_type,
        trade_type,
        block_timestamp,
        amount,
        amount_usd,
        fee_amount,
        base_delta_amount,
        quote_delta_amount,
        _inserted_timestamp
    FROM
        {{ ref('silver__vertex_perps') }}

{% if is_incremental() %}
WHERE
    subaccount IN (
        SELECT
            subaccount
        FROM
            new_subaccount_actions
    )
{% endif %}
UNION ALL
SELECT
    subaccount,
    trader,
    digest,
    'spot' AS product_type,
    trade_type,
    block_timestamp,
    amount,
    amount_usd,
    fee_amount,
    base_delta_amount,
    quote_delta_amount,
    _inserted_timestamp
FROM
    {{ ref('silver__vertex_spot') }}

{% if is_incremental() %}
WHERE
    subaccount IN (
        SELECT
            subaccount
        FROM
            new_subaccount_actions
    )
{% endif %}
),
liquidations AS (
    SELECT
        trader,
        subaccount,
        SUM(amount) AS total_liquidation_amount,
        SUM(amount_quote) AS total_liquidation_amount_quote,
        COUNT(*) AS liquidation_count
    FROM
        {{ ref('silver__vertex_liquidations') }}

{% if is_incremental() %}
WHERE
    subaccount IN (
        SELECT
            subaccount
        FROM
            new_subaccount_actions
    )
{% endif %}
GROUP BY
    1,
    2
),
FINAL AS (
    SELECT
        t.subaccount,
        t.trader,
        --ROW_NUMBER() over(ORDER BY sum(amount_usd) DESC) AS usd_volume_rank, not sure how to incorp w/ incremental, maybe just remove
        MIN(
            t.block_timestamp
        ) AS first_trade_timestamp,
        MAX(
            t.block_timestamp
        ) AS last_trade_timestamp,
        DATEDIFF(
            'day',
            first_trade_timestamp,
            last_trade_timestamp
        ) AS account_age,
        COUNT(DISTINCT(digest)) AS trade_count,
        SUM(
            CASE
                WHEN product_type = 'perp' THEN + 1
                ELSE 0
            END
        ) AS perp_trade_count,
        SUM(
            CASE
                WHEN product_type = 'spot' THEN + 1
                ELSE 0
            END
        ) AS spot_trade_count,
        SUM(
            CASE
                WHEN trade_type = 'buy/long' THEN + 1
                ELSE 0
            END
        ) AS long_count,
        SUM(
            CASE
                WHEN trade_type = 'sell/short' THEN + 1
                ELSE 0
            END
        ) AS short_count,
        SUM(amount_usd) AS total_usd_volume,
        AVG(amount_usd) AS avg_usd_trade_size,
        SUM(fee_amount) AS total_fee_amount,
        SUM(base_delta_amount) AS total_base_delta_amount,
        SUM(quote_delta_amount) AS total_quote_delta_amount,
        MAX(
            l.total_liquidation_amount
        ) AS total_liquidation_amount,
        MAX(
            l.total_liquidation_amount_quote
        ) AS total_liquidation_amount_quote,
        MAX(liquidation_count) AS total_liquidation_count,
        MAX(
            t._inserted_timestamp
        ) AS _inserted_timestamp
    FROM
        trades_union t
        LEFT JOIN liquidations l
        ON t.trader = l.trader
        AND t.subaccount = l.subaccount
    GROUP BY
        1,
        2
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
