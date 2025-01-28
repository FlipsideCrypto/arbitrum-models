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
        {{ ref('silver_dex__gmx_v2_events') }}
    WHERE
        event_name IN (
            'SwapInfo',
            'OrderExecuted'
        )

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(
            modified_timestamp
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
        modified_timestamp,
        event_name,
        event_name_hash,
        msg_sender,
        topic_1,
        topic_2,
        event_data[0][0][0][1] :: STRING AS market,
        event_data[0][0][1][1] :: STRING AS receiver,
        event_data[0][0][2][1] :: STRING AS token_in,
        event_data[0][0][3][1] :: STRING AS token_out,
        TRY_TO_NUMBER(event_data[1][0][0][1] :: STRING) AS token_in_price,
        TRY_TO_NUMBER(event_data[1][0][1][1] :: STRING) AS token_out_price,
        TRY_TO_NUMBER(event_data[1][0][2][1] :: STRING) AS amount_in,
        TRY_TO_NUMBER(event_data[1][0][3][1] :: STRING) AS amount_in_after_fees,
        TRY_TO_NUMBER(event_data[1][0][4][1] :: STRING) AS amount_out,
        TRY_TO_NUMBER(event_data[2][0][0][1] :: STRING) AS price_impact_usd,
        TRY_TO_NUMBER(event_data[2][0][0][1] :: STRING) AS price_impact_amount,
        event_data[4][0][0][1] :: STRING AS key
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
        modified_timestamp,
        event_name,
        event_name_hash,
        msg_sender as sender,
        receiver as tx_to,
        topic_1,
        market,
        receiver,
        token_in,
        token_in_price AS raw_token_in_price,
        amount_in AS raw_amount_in,
        amount_in_after_fees,
        c1.token_symbol AS token_in_symbol,
        c1.token_decimals AS token_in_decimals,
        token_out,
        token_out_price AS raw_token_out_price,
        amount_out AS raw_amount_out,
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
        event_data [4] [0] [0] [1] :: STRING AS key
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
    p.token_symbol,
    p.contract_address AS underlying_address,
    receiver,
    sender,
    tx_to,
    CASE
        WHEN e.key IS NOT NULL THEN 'executed'
        ELSE 'not-executed'
    END AS order_execution,
    token_in,
    token_in_symbol,
    p.token_decimals AS token_in_decimals,
    raw_token_in_price AS token_in_price_unadj,
    raw_token_in_price :: INT / pow(
        10,
        (30 - token_in_decimals)
    ) AS token_in_price,
    raw_amount_in AS amount_in_unadj,
    raw_amount_in :: INT / pow(
        10,
        token_in_decimals
    ) AS amount_in,
    amount_in*token_in_price AS amount_in_usd,
    amount_in_after_fees AS amount_in_after_fees_unadj,
    amount_in_after_fees :: INT / pow(
        10,
        token_in_decimals
    ) AS amount_in_after_fees,
    token_out,
    token_out_symbol,
    q.token_decimals AS token_out_decimals,
    raw_token_out_price AS token_out_price_unadj,
    raw_token_out_price :: INT / pow(
        10,
        (30-token_out_decimals)
    ) AS token_out_price,
    raw_amount_out AS amount_out_unadj,
    raw_amount_out :: INT / pow(
        10,
        token_out_decimals
    ) AS amount_out,
    amount_out*token_out_price AS amount_out_usd,
    price_impact_usd AS price_impact_usd_unadj,
    price_impact_usd :: INT / pow(
        10,
        30
    ) AS price_impact_usd,
    price_impact_amount AS price_impact_amount_unadj,
    price_impact_amount :: INT /pow(
        10,
        30
    ) AS price_impact_amount,
    'gmx-v2' AS platform,
    'Swap' AS event_name,
    A.key,
    A._log_id,
    A.modified_timestamp
FROM
    column_format A
INNER JOIN executed_orders e
    ON A.key = e.key
LEFT JOIN 
    contracts p ON p.contract_address=token_in
LEFT JOIN
    contracts q ON q.contract_address=token_in