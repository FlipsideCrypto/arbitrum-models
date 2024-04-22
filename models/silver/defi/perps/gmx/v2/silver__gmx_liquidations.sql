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
        event_name in ('PositionDecrease')
    AND
        event_data[1][0][16][1]::INT = 7
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
        topic_1,
        topic_2,
        event_data[0][0][0][1] as account,
        event_data[0][0][1][1] as market,
        event_data[0][0][2][1] as collateral_token,
        event_data[1][0][0][1] as size_in_usd,
        event_data[1][0][1][1] as size_in_tokens,
        event_data[1][0][2][1] as collateral_amount,
        event_data[1][0][3][1] as borrowing_factor,
        event_data[1][0][4][1] as funding_fee_amount_per_size,
        event_data[1][0][5][1] as long_token_claimable_funding_amount_per_size,
        event_data[1][0][6][1] as short_token_claimable_funding_amount_per_size,
        event_data[1][0][7][1] as execution_price,
        event_data[1][0][8][1] as max_index_token_price,
        event_data[1][0][9][1] as min_index_token_price,
        event_data[1][0][10][1] as max_collateral_token_price,
        event_data[1][0][11][1] as min_collateral_token_price,
        event_data[1][0][12][1] as size_delta_usd,
        event_data[1][0][13][1] as size_delta_amount,
        event_data[1][0][14][1] as collateral_delta_amount,
        event_data[1][0][15][1] as price_impact_diff_usd,
        event_data[1][0][16][1] as order_type,
        event_data[2][0][0][1] as price_impact_usd,
        event_data[2][0][1][1] as base_pnl_usd,
        event_data[2][0][2][1] as uncapped_base_pnl_usd,
        event_data[3][0][0][1] as is_long,
        event_data[4][0][0][1] as order_key,
        event_data[4][0][1][1] as position_key
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
    a.block_number,
    a.block_timestamp,
    a.tx_hash,
    a.origin_function_signature,
    a.origin_from_address,
    a.origin_to_address,
    a.contract_address,
    a.event_index,
    a._log_id,
    a._inserted_timestamp,
    a.event_name,
    a.event_name_hash,
    a.msg_sender,
    a.topic_1,
    a.topic_2,
    account,
    market,
    p.symbol,
    p.address as underlying_address,
    collateral_token,
    c.token_symbol as collateral_token_symbol,
    borrowing_factor as borrowing_factor_unadj,
    borrowing_factor :: FLOAT / pow(
        10,
        30
    ) as borrowing_factor,
    funding_fee_amount_per_size,
    execution_price as execution_price_unadj,
    execution_price :: FLOAT / pow(
        10,
        30
    ) as execution_price,
    size_delta_usd AS size_delta_usd_unadj,
    size_delta_usd :: FLOAT / pow(
        10,
        30
    ) AS size_delta_usd,
    size_delta_amount AS size_delta_amount_unadj,
    size_delta_amount :: FLOAT / pow(
        10,
        c.token_decimals
    ) AS size_delta_amount,
    collateral_delta_amount as collateral_delta_amount_unadj,
    collateral_delta_amount :: INT / pow(
        10,
        c.token_decimals
    ) AS collateral_delta_amount,
    price_impact_diff_usd as price_impact_diff_usd_unadj,
    price_impact_diff_usd :: FLOAT / pow(
        10,
        30
    ) AS price_impact_diff_usd,
    order_type,
    price_impact_usd as price_impact_usd_unadj,
    price_impact_usd :: FLOAT / pow(
        10,
        30
    ) AS price_impact_usd,
    base_pnl_usd as base_pnl_usd_unadj,
    base_pnl_usd :: FLOAT / pow(
        10,
        30
    ) AS base_pnl_usd,
    uncapped_base_pnl_usd as uncapped_base_pnl_usd_unadj,
    uncapped_base_pnl_usd :: FLOAT / pow(
        10,
        30
    ) AS uncapped_base_pnl_usd,
    is_long,
    order_key,
    position_key,
    a._log_id,
    a._inserted_timestamp
FROM
    parse_event_data a
LEFT JOIN 
    contracts c
ON
    c.contract_address = a.collateral_token
LEFT JOIN
    {{ ref('silver__gmx_dim_products_v2') }} p 
ON
    p.market_address = a.market