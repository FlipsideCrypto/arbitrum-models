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
        {{ ref('silver_perps__gmxv2_events') }}
    WHERE
        event_name IN (
            'OrderCreated',
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
        A.block_number,
        A.block_timestamp,
        A.tx_hash,
        A.origin_function_signature,
        A.origin_from_address,
        A.origin_to_address,
        A.contract_address,
        A.event_index,
        A._log_id,
        A._inserted_timestamp,
        event_name,
        event_name_hash,
        msg_sender,
        topic_1,
        topic_2,
        event_data [0] [0] [0] [1] :: STRING AS account,
        event_data [0] [0] [1] [1] :: STRING AS receiver,
        event_data [0] [0] [2] [1] :: STRING AS call_back_contract,
        event_data [0] [0] [3] [1] :: STRING AS ui_fee_receiver,
        event_data [0] [0] [4] [1] :: STRING AS market,
        p.symbol,
        p.decimals,
        p.address AS underlying_address,
        event_data [0] [0] [5] [1] :: STRING AS initial_collateral_token,
        event_data [0] [1] [0] [1] [0] :: STRING AS swap_path,
        event_data [1] [0] [0] [1] :: INT AS order_type,
        event_data [1] [0] [1] [1] AS decrease_position_swap_type,
        event_data [1] [0] [2] [1] AS size_delta_usd,
        event_data [1] [0] [3] [1] AS initial_collateral_delta_amount,
        event_data [1] [0] [4] [1] AS trigger_price,
        event_data [1] [0] [5] [1] AS acceptable_price,
        event_data [1] [0] [6] [1] AS execution_fee,
        event_data [1] [0] [7] [1] AS call_back_gas_limit,
        event_data [1] [0] [8] [1] AS min_output_amount,
        event_data [1] [0] [9] [1] :: INT AS updated_at_block,
        event_data [3] [0] [0] [1] AS is_long,
        event_data [3] [0] [1] [1] AS should_unwrap_native_token,
        event_data [3] [0] [2] [1] AS is_frozen,
        event_data [4] [0] [0] [1] AS key,
    FROM
        gmx_events A
        LEFT JOIN {{ ref('silver_perps__gmxv2_dim_products') }}
        p
        ON p.market_address = event_data [0] [0] [4] [1] :: STRING
    WHERE
        event_name = 'OrderCreated'
        AND event_data [1] [0] [0] [1] :: INT NOT IN (
            7,
            0,
            1
        ) --liquidation & Swap increase + decrease
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
                DISTINCT(initial_collateral_token)
            FROM
                parse_data
            UNION
            SELECT
                DISTINCT(underlying_address)
            FROM
                parse_data
        )
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
    event_name,
    account,
    receiver,
    market,
    symbol,
    underlying_address,
    event_name_hash,
    topic_1,
    initial_collateral_token,
    C.token_symbol AS initial_collateral_token_symbol,
    order_type as order_type_raw,
    CASE
        WHEN order_type = 2 THEN 'market_increase'
        WHEN order_type = 3 THEN 'limit_increase'
        WHEN order_type = 4 THEN 'market_decrease'
        WHEN order_type = 5 THEN 'limit_decrease'
        WHEN order_type = 6 THEN 'stop_loss_decrease'
    END AS order_type,
    CASE
        WHEN e.key IS NOT NULL THEN 'executed'
        ELSE 'not-executed'
    END AS order_execution,
    decrease_position_swap_type :: INT as market_reduce_flag,
    size_delta_usd AS size_delta_usd_unadj,
    size_delta_usd :: FLOAT / pow(
        10,
        30
    ) AS size_delta_usd,
    initial_collateral_delta_amount AS initial_collateral_delta_amount_unadj,
    initial_collateral_delta_amount :: FLOAT / pow(
        10,
        C.token_decimals
    ) AS initial_collateral_delta_amount,
    trigger_price AS trigger_price_unadj,
    trigger_price :: FLOAT / pow(10, (30 - decimals)) AS trigger_price,
    acceptable_price AS acceptable_price_unadj,
    acceptable_price :: FLOAT / pow(10, (30 - decimals)) AS acceptable_price,
    execution_fee AS execution_fee_unadj,
    execution_fee :: FLOAT / pow(
        10,
        18
    ) AS execution_fee,
    updated_at_block,
    is_long :: BOOLEAN as is_long,
    should_unwrap_native_token :: BOOLEAN as should_unwrap_native_token,
    is_frozen :: BOOLEAN as is_frozen,
    A.key,
    A._log_id,
    A._inserted_timestamp,
    sysdate() as modified_timestamp
FROM
    parse_data A
    LEFT JOIN executed_orders e
    ON A.key = e.key
    LEFT JOIN contracts C
    ON C.contract_address = A.initial_collateral_token
