{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['tx_hash', 'event_index'],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH gmx_symmio AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        event_name,
        event_index,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        trader,
        trade_type,
        platform,
        symbol,
        market_type,
        is_taker,
        price_amount_unadj,
        price_amount,
        amount_unadj,
        amount,
        amount_usd,
        fee_amount_unadj,
        fee_amount,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver_perps__complete_perp_trades') }} A
    WHERE
        origin_to_address IN (
            '0xddba98640ba9c19fb3838d7982de798c1ed301df',-- gmx v2
            '0x6273242a7e88b3de90822b31648c212215caafe4' -- symmio
        )
{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
),
vertex AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        event_name,
        event_index,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        trader,
        trade_type,
        'vertex' AS platform,
        REPLACE(
            symbol,
            '-PERP',
            ''
        ) AS symbol,
        CASE
            WHEN market_reduce_flag = TRUE THEN 'market_decrease'
            ELSE 'market_increase'
        END AS market_type,
        is_taker,
        price_amount_unadj,
        price_amount,
        amount_unadj,
        amount,
        amount_usd,
        fee_amount_unadj,
        fee_amount,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__vertex_perps') }} A
    WHERE
        utils.udf_hex_to_string(SUBSTR(subaccount, 43, 8)) = 'pear'
{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}

),
FINAL AS (
    SELECT
        *
    FROM
        gmx_symmio
    UNION ALL
    SELECT
        *
    FROM
        vertex
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    contract_address,
    event_name,
    event_index,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    trader,
    trade_type,
    platform,
    symbol,
    market_type,
    is_taker,
    price_amount_unadj,
    price_amount,
    amount_unadj,
    amount,
    amount_usd,
    fee_amount_unadj,
    fee_amount,
    _log_id,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'event_index']
    ) }} AS pear_perps_id,
    SYSDATE() AS modified_timestamp,
    SYSDATE() AS inserted_timestamp,
    '{{invocation_id}}' AS _invocation_id
FROM
    FINAL
