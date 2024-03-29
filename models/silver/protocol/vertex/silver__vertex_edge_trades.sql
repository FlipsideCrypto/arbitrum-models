{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

with perp_trades as (
    SELECT  
        *
    FROM
        {{ ref('silver__vertex_perps') }} p 
    WHERE 
    1=1
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
edge_trades as (
    SELECT
        event_index - 1 as trader_event,
        *
    FROM 
        perp_trades
    WHERE TRADER = '0x0000000000000000000000000000000000000000'
),
final as (
    SELECT 
        e.block_number,
        e.block_timestamp,
        e.tx_hash,
        e.event_index as edge_event_index,
        p.event_index as user_event_index,
        e.digest as edge_digest
        p.digest as user_digest,
        p.trader,
        p.subaccount
        p.symbol,
        e.order_type as edge_order_type,
        p.order_type as edge_order_type,
        e.is_taker as edge_is_taker,
        p.is_taker as user_is_taker,
        e.trade_type as edge_trade_type,
        p.trade_type as user_trade_type,
        e.amount_usd as edge_amount_usd,
        p.amount_usd as user_amount_usd,
        e.quote_delta_amount as edge_quote_delta,
        p.quote_delta_amount as user_quote_delta,
        e.base_delta_amount as edge_base_delta,
        p.base_delta_amount as user_base_delta,
        e._log_id,
        e._inserted_timestamp
    FROM
        edge_trades e
    LEFT JOIN
        (SELECT * FROM perp_trades WHERE TRADER <> '0x0000000000000000000000000000000000000000') p
    ON
        e.TX_HASH = p.tx_hash
    AND
        e.trader_event = p.event_index 
    AND
        e.product_id = p.PRODUCT_ID
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
