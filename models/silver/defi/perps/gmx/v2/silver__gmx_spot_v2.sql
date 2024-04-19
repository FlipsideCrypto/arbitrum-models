{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH gmx_events AS (

    SELECT
        *
    FROM
        {{ ref('silver__gmx_events_v2') }}
    WHERE
        event_name IN (
            'SwapInfo',
            'OrderExecuted'
        )

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
lat_flat AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        event_index,
        _log_id,
        _inserted_timestamp,
        event_name,
        event_name_hash,
        msg_sender,
        topic_1,
        topic_2,
        event.value [0] :: variant AS event_data
    FROM
        gmx_events,
        LATERAL FLATTEN(
            input => event_data
        ) event
    WHERE
        event_name = 'SwapInfo'
),
lat_flat_2 AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        event_index,
        _log_id,
        _inserted_timestamp,
        event_name,
        event_name_hash,
        msg_sender,
        topic_1,
        UPPER(
            VALUE [0] :: STRING
        ) AS key,
        VALUE [1] :: STRING AS VALUE
    FROM
        lat_flat,
        LATERAL FLATTEN (
            input => event_data
        )
),
pivot AS (
    SELECT
        *
    FROM
        lat_flat_2 pivot (MAX(VALUE) FOR key IN('MARKET', 'RECEIVER', 'TOKENIN', 'TOKENOUT', 'TOKENINPRICE', 'TOKENOUTPRICE', 'AMOUNTIN', 'AMOUNTINAFTERFEES', 'AMOUNTOUT', 'PRICEIMPACTUSD', 'PRICEIMPACTAMOUNT', 'ORDERKEY'))
),
contracts AS (
    SELECT
        contract_address,
        token_symbol,
        token_decimals
    FROM
        {{ ref('silver__contracts') }}
    WHERE
        contract_address IN (
            SELECT
                DISTINCT("'TOKENIN'")
            FROM
                pivot
            UNION ALL
            SELECT
                DISTINCT("'TOKENOUT'")
            FROM
                pivot
        )
),
column_format AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        p.contract_address,
        event_index,
        _log_id,
        _inserted_timestamp,
        event_name,
        event_name_hash,
        msg_sender,
        topic_1,
        "'MARKET'" AS market,
        "'RECEIVER'" AS reciever,
        "'TOKENIN'" AS token_in,
        "'TOKENINPRICE'" AS token_in_price,
        "'AMOUNTIN'" AS amount_in,
        "'AMOUNTINAFTERFEES'" AS amount_in_after_fees,
        c1.token_symbol AS token_in_symbol,
        c1.token_decimals AS token_in_decimals,
        "'TOKENOUT'" AS token_out,
        "'TOKENOUTPRICE'" AS token_out_price,
        "'AMOUNTOUT'" AS amount_out,
        c2.token_symbol AS token_out_symbol,
        c2.token_decimals AS token_out_decimals,
        "'PRICEIMPACTUSD'" AS price_impact_usd,
        "'PRICEIMPACTAMOUNT'" AS price_impact_amount,
        "'ORDERKEY'" AS key
    FROM
        pivot p
        LEFT JOIN contracts c1
        ON c1.contract_address = p."'TOKENIN'"
        LEFT JOIN contracts c2
        ON c2.contract_address = p."'TOKENOUT'"
),
executed_orders AS (
    SELECT
        event_data [4] [0] [0] [1] AS key
    FROM
        gmx_events
    WHERE
        event_name = 'OrderExecuted'
)
SELECT
    A.block_number,
    A.block_timestamp,
    A.tx_hash,
    A.origin_function_signature,
    A.origin_from_address,
    A.origin_to_address,
    A.contract_address,
    A.event_index,
    market,
    p.symbol,
    p.address AS underlying_address,
    reciever,
    CASE
        WHEN e.key IS NOT NULL THEN 'executed'
        ELSE 'not-executed'
    END AS order_execution,
    token_in,
    token_in_symbol,
    token_in_price AS token_in_price_unadj,
    token_in_price :: INT / pow(
        10,
        token_in_decimals
    ) AS token_in_price,
    amount_in AS amount_in_unadj,
    amount_in :: INT / pow(
        10,
        token_in_decimals
    ) AS amount_in,
    amount_in_after_fees AS amount_in_after_fees_unadj,
    amount_in_after_fees :: INT / pow(
        10,
        token_in_decimals
    ) AS amount_in_after_fees,
    token_out,
    token_out_symbol,
    token_out_price AS token_out_price_unadj,
    token_out_price :: INT / pow(
        10,
        token_out_decimals
    ) AS token_out_price,
    amount_out AS amount_out_unadj,
    amount_out :: INT / pow(
        10,
        token_out_decimals
    ) AS amount_out,
    price_impact_usd,
    price_impact_amount,
    A.key,
    A._log_id,
    A._inserted_timestamp
FROM
    column_format A
    LEFT JOIN executed_orders e
    ON A.key = e.key
    LEFT JOIN {{ ref('silver__gmx_dim_products_v2') }}
    p
    ON p.market_address = A.market
