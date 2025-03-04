{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = '_log_id',
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
        topic_0,
        topic_1,
        topic_2,
        event_index,
        event_name,
        event_name_hash,
        msg_sender,
        event_data,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver_perps__gmxv2_events') }}
    WHERE
        event_name IN ('PositionDecrease')
        AND event_data [1] [0] [16] [1] :: INT = 7

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
parse_event_data AS (
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
        topic_0,
        topic_1,
        topic_2,
        event_data [0] [0] [0] [1] :: STRING AS account,
        event_data [0] [0] [1] [1] :: STRING AS market,
        event_data [0] [0] [2] [1] :: STRING AS collateral_token,
        TRY_TO_NUMBER(
            event_data [1] [0] [0] [1] :: STRING
        ) AS size_in_usd,
        TRY_TO_NUMBER(
            event_data [1] [0] [1] [1] :: STRING
        ) AS size_in_tokens,
        TRY_TO_NUMBER(
            event_data [1] [0] [2] [1] :: STRING
        ) AS collateral_amount,
        TRY_TO_NUMBER(
            event_data [1] [0] [3] [1] :: STRING
        ) AS borrowing_factor,
        TRY_TO_NUMBER(
            event_data [1] [0] [4] [1] :: STRING
        ) AS funding_fee_amount_per_size,
        TRY_TO_NUMBER(
            event_data [1] [0] [5] [1] :: STRING
        ) AS long_token_claimable_funding_amount_per_size,
        TRY_TO_NUMBER(
            event_data [1] [0] [6] [1] :: STRING
        ) AS short_token_claimable_funding_amount_per_size,
        TRY_TO_NUMBER(
            event_data [1] [0] [7] [1] :: STRING
        ) AS execution_price,
        TRY_TO_NUMBER(
            event_data [1] [0] [8] [1] :: STRING
        ) AS max_index_token_price,
        TRY_TO_NUMBER(
            event_data [1] [0] [9] [1] :: STRING
        ) AS min_index_token_price,
        TRY_TO_NUMBER(
            event_data [1] [0] [10] [1] :: STRING
        ) AS max_collateral_token_price,
        TRY_TO_NUMBER(
            event_data [1] [0] [11] [1] :: STRING
        ) AS min_collateral_token_price,
        TRY_TO_NUMBER(
            event_data [1] [0] [12] [1] :: STRING
        ) AS size_delta_usd,
        TRY_TO_NUMBER(
            event_data [1] [0] [13] [1] :: STRING
        ) AS size_delta_amount,
        TRY_TO_NUMBER(
            event_data [1] [0] [14] [1] :: STRING
        ) AS collateral_delta_amount,
        TRY_TO_NUMBER(
            event_data [1] [0] [15] [1] :: STRING
        ) AS price_impact_diff_usd,
        TRY_TO_NUMBER(
            event_data [1] [0] [16] [1] :: STRING
        ) AS order_type,
        TRY_TO_NUMBER(
            event_data [2] [0] [0] [1] :: STRING
        ) AS price_impact_usd,
        TRY_TO_NUMBER(
            event_data [2] [0] [1] [1] :: STRING
        ) AS base_pnl_usd,
        TRY_TO_NUMBER(
            event_data [2] [0] [2] [1] :: STRING
        ) AS uncapped_base_pnl_usd,
        event_data [3] [0] [0] [1] :: BOOLEAN AS is_long,
        event_data [4] [0] [0] [1] :: STRING AS order_key,
        event_data [4] [0] [1] [1] :: STRING AS position_key
    FROM
        gmx_events
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
                DISTINCT(collateral_token)
            FROM
                parse_event_data
        )
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
    A.event_name,
    A.event_name_hash,
    A.msg_sender,
    A.topic_1,
    A.topic_2,
    account,
    market,
    p.symbol,
    p.address AS underlying_address,
    collateral_token,
    C.token_symbol AS collateral_token_symbol,
    borrowing_factor AS borrowing_factor_unadj,
    borrowing_factor :: FLOAT / pow(
        10,
        30
    ) AS borrowing_factor,
    funding_fee_amount_per_size,
    execution_price AS execution_price_unadj,
    execution_price :: FLOAT / pow(10, (30 - decimals)) AS execution_price,
    size_delta_usd AS size_delta_usd_unadj,
    size_delta_usd :: FLOAT / pow(
        10,
        30
    ) AS size_delta_usd,
    size_delta_amount AS size_delta_amount_unadj,
    size_delta_amount :: FLOAT / pow(
        10,
        p.decimals
    ) AS size_delta_amount,
    collateral_delta_amount AS collateral_delta_amount_unadj,
    collateral_delta_amount :: INT / pow(
        10,
        C.token_decimals
    ) AS collateral_delta_amount,
    price_impact_diff_usd AS price_impact_diff_usd_unadj,
    price_impact_diff_usd :: FLOAT / pow(
        10,
        30
    ) AS price_impact_diff_usd,
    order_type,
    price_impact_usd AS price_impact_usd_unadj,
    price_impact_usd :: FLOAT / pow(
        10,
        30
    ) AS price_impact_usd,
    base_pnl_usd AS base_pnl_usd_unadj,
    base_pnl_usd :: FLOAT / pow(
        10,
        30
    ) AS base_pnl_usd,
    uncapped_base_pnl_usd AS uncapped_base_pnl_usd_unadj,
    uncapped_base_pnl_usd :: FLOAT / pow(
        10,
        30
    ) AS uncapped_base_pnl_usd,
    is_long,
    order_key,
    position_key,
    A._log_id,
    A._inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    parse_event_data A
    LEFT JOIN contracts C
    ON C.contract_address = A.collateral_token
    LEFT JOIN {{ ref('silver_perps__gmxv2_dim_products') }}
    p
    ON p.market_address = A.market
