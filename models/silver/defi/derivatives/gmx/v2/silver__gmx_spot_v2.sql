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
parse_data AS (
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
        event_data[0][0][0][1] as market,
        event_data[0][0][1][1] as receiver,
        event_data[0][0][2][1] as token_in,
        event_data[0][0][3][1] as token_out,
        event_data[1][0][0][1] as token_in_price,
        event_data[1][0][1][1] as token_out_price,
        event_data[1][0][2][1] as amount_in,
        event_data[1][0][3][1] as amount_in_after_fees,
        event_data[1][0][4][1] as amount_out,
        event_data[2][0][0][1] as price_impact_usd,
        event_data[2][0][0][1] as price_impact_amount,
        event_data[4][0][0][1] as key
    FROM
        gmx_events
    WHERE
        event_name = 'SwapInfo'
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
                DISTINCT(token_in)
            FROM
                parse_data
            UNION ALL
            SELECT
                DISTINCT(token_out)
            FROM
                parse_data
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
        market,
        receiver,
        token_in,
        token_in_price,
        amount_in,
        amount_in_after_fees,
        c1.token_symbol AS token_in_symbol,
        c1.token_decimals AS token_in_decimals,
        token_out,
        token_out_price,
        amount_out,
        c2.token_symbol AS token_out_symbol,
        c2.token_decimals AS token_out_decimals,
        price_impact_usd,
        price_impact_amount,
        key
    FROM
        parse_data p
        LEFT JOIN contracts c1
        ON c1.contract_address = p.token_in
        LEFT JOIN contracts c2
        ON c2.contract_address = p.token_out
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
    receiver,
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
        (30-p.decimals)
    ) AS token_out_price,
    amount_out AS amount_out_unadj,
    amount_out :: INT / pow(
        10,
        p.decimals
    ) AS amount_out,
    price_impact_usd as price_impact_usd_unadj,
    price_impact_usd :: INT / pow(
        10,
        30
    ) AS price_impact_usd,
    price_impact_amount AS price_impact_amount_unadj,
    price_impact_amount ::INT /pow(
        10,
        30
    ) as price_impact_amount,
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
