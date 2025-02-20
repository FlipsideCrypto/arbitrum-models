{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH gmx_events AS (

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
        event_data
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
AND modified_timestamp >= SYSDATE() - INTERVAL '7 day'
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
        event_data [0] [0] [0] [1] :: STRING AS market,
        event_data [0] [0] [1] [1] :: STRING AS receiver,
        event_data [0] [0] [2] [1] :: STRING AS token_in,
        event_data [0] [0] [3] [1] :: STRING AS token_out,
        TRY_TO_NUMBER(
            event_data [1] [0] [0] [1] :: STRING
        ) AS token_in_price,
        TRY_TO_NUMBER(
            event_data [1] [0] [1] [1] :: STRING
        ) AS token_out_price,
        TRY_TO_NUMBER(
            event_data [1] [0] [2] [1] :: STRING
        ) AS amount_in,
        TRY_TO_NUMBER(
            event_data [1] [0] [3] [1] :: STRING
        ) AS amount_in_after_fees,
        TRY_TO_NUMBER(
            event_data [1] [0] [4] [1] :: STRING
        ) AS amount_out,
        TRY_TO_NUMBER(
            event_data [2] [0] [0] [1] :: STRING
        ) AS price_impact_usd,
        TRY_TO_NUMBER(
            event_data [2] [0] [0] [1] :: STRING
        ) AS price_impact_amount,
        event_data [4] [0] [0] [1] :: STRING AS key
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
        {{ ref('core__dim_contracts') }}
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
        msg_sender AS sender,
        receiver AS tx_to,
        topic_1,
        market,
        receiver,
        token_in,
        token_in_price AS raw_token_in_price,
        amount_in AS amount_in_unadj,
        amount_in_after_fees,
        token_out,
        token_out_price AS raw_token_out_price,
        amount_out AS amount_out_unadj,
        price_impact_usd,
        price_impact_amount,
        key
    FROM
        parse_data p
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
    receiver,
    sender,
    tx_to,
    CASE
        WHEN e.key IS NOT NULL THEN 'executed'
        ELSE 'not-executed'
    END AS order_execution,
    token_in,
    amount_in_unadj,
    token_out,
    amount_out_unadj,
    'gmx-v2' AS platform,
    'Swap' AS event_name,
    A.key,
    A._log_id,
    A.modified_timestamp
FROM
    column_format A
    INNER JOIN executed_orders e
    ON A.key = e.key
