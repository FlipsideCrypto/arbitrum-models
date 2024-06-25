{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH perp_trades AS (

    SELECT
        *
    FROM
        {{ ref('silver__vertex_perps') }}
        p
    WHERE
        1 = 1

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
),
edge_trades AS (
    SELECT
        event_index - 1 AS trader_event_before,
        event_index + 1 AS trader_event_after,*
    FROM
        perp_trades
    WHERE
        trader = '0x0000000000000000000000000000000000000000'
),
FINAL AS (
    SELECT
        e.block_number,
        e.block_timestamp,
        e.tx_hash,
        e.event_index AS edge_event_index,
        e.trader_event_before AS user_event_index,
        e.digest AS edge_digest,
        p.digest AS user_digest,
        p.trader,
        p.subaccount,
        p.symbol,
        e.order_type AS edge_order_type,
        p.order_type AS user_order_type,
        e.is_taker AS edge_is_taker,
        p.is_taker AS user_is_taker,
        e.trade_type AS edge_trade_type,
        p.trade_type AS user_trade_type,
        e.price_amount_unadj AS edge_price_amount_unadj,
        p.price_amount_unadj AS user_price_amount_unadj,
        e.price_amount AS edge_price_amount,
        p.price_amount AS user_price_amount,
        e.amount_unadj AS edge_amount_unadj,
        p.amount_unadj AS user_amount_unadj,
        e.amount AS edge_amount,
        p.amount AS user_amount,
        e.amount_usd AS edge_amount_usd,
        p.amount_usd AS user_amount_usd,
        e.fee_amount_unadj AS edge_fee_amount_unadj,
        p.fee_amount_unadj AS user_fee_amount_unadj,
        e.fee_amount AS edge_fee_amount,
        p.fee_amount AS user_fee_amount,
        e.base_delta_amount_unadj AS edge_base_delta_amount_unadj,
        p.base_delta_amount_unadj AS user_base_delta_amount_unadj,
        e.base_delta_amount AS edge_base_delta_amount,
        p.base_delta_amount AS user_base_delta_amount,
        e.quote_delta_amount_unadj AS edge_quote_delta_amount_unadj,
        p.quote_delta_amount_unadj AS user_quote_delta_amount_unadj,
        e.quote_delta_amount AS edge_quote_delta_amount,
        p.quote_delta_amount AS user_quote_delta_amount,
        e._log_id,
        e._inserted_timestamp
    FROM
        edge_trades e
        LEFT JOIN (
            SELECT
                *
            FROM
                perp_trades
            WHERE
                trader <> '0x0000000000000000000000000000000000000000'
        ) p
        ON e.tx_hash = p.tx_hash
        AND e.trader_event_before = p.event_index
        AND e.product_id = p.product_id
    WHERE
        user_digest IS NOT NULL
    UNION ALL
    SELECT
        e.block_number,
        e.block_timestamp,
        e.tx_hash,
        e.event_index AS edge_event_index,
        e.trader_event_after AS user_event_index,
        e.digest AS edge_digest,
        p.digest AS user_digest,
        p.trader,
        p.subaccount,
        p.symbol,
        e.order_type AS edge_order_type,
        p.order_type AS user_order_type,
        e.is_taker AS edge_is_taker,
        p.is_taker AS user_is_taker,
        e.trade_type AS edge_trade_type,
        p.trade_type AS user_trade_type,
        e.price_amount_unadj AS edge_price_amount_unadj,
        p.price_amount_unadj AS user_price_amount_unadj,
        e.price_amount AS edge_price_amount,
        p.price_amount AS user_price_amount,
        e.amount_unadj AS edge_amount_unadj,
        p.amount_unadj AS user_amount_unadj,
        e.amount AS edge_amount,
        p.amount AS user_amount,
        e.amount_usd AS edge_amount_usd,
        p.amount_usd AS user_amount_usd,
        e.fee_amount_unadj AS edge_fee_amount_unadj,
        p.fee_amount_unadj AS user_fee_amount_unadj,
        e.fee_amount AS edge_fee_amount,
        p.fee_amount AS user_fee_amount,
        e.base_delta_amount_unadj AS edge_base_delta_amount_unadj,
        p.base_delta_amount_unadj AS user_base_delta_amount_unadj,
        e.base_delta_amount AS edge_base_delta_amount,
        p.base_delta_amount AS user_base_delta_amount,
        e.quote_delta_amount_unadj AS edge_quote_delta_amount_unadj,
        p.quote_delta_amount_unadj AS user_quote_delta_amount_unadj,
        e.quote_delta_amount AS edge_quote_delta_amount,
        p.quote_delta_amount AS user_quote_delta_amount,
        e._log_id,
        e._inserted_timestamp
    FROM
        edge_trades e
        LEFT JOIN (
            SELECT
                *
            FROM
                perp_trades
            WHERE
                trader <> '0x0000000000000000000000000000000000000000'
        ) p
        ON e.tx_hash = p.tx_hash
        AND e.trader_event_after = p.event_index
        AND e.product_id = p.product_id
    WHERE
        user_digest IS NOT NULL
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash','edge_event_index']
    ) }} AS vertex_edge_trade_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
