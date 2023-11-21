{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['block_number','platform'],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['non_realtime','reorg','curated']
) }}

WITH withdraws AS (

    SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    aave_token AS protocol_market,
    aave_market AS token_address,
    symbol as token_symbol,
    amount_unadj,
    withdrawn_tokens AS amount,
    depositor_address,
    'Aave V3' AS platform,
    'arbitrum' AS blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__aave_withdraws') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) - INTERVAL '36 hours'
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
    SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    radiant_token AS protocol_market,
    radiant_market AS token_address,
    symbol as token_symbol,
    amount_unadj,
    withdrawn_tokens AS amount,
    depositor_address,
    'Radiant' AS platform,
    'arbitrum' AS blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__radiant_withdraws') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) - INTERVAL '36 hours'
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    compound_market AS protocol_market,
    token_address,
    token_symbol,
    amount_unadj,
    amount,
    depositor_address,
    compound_version AS platform,
    'arbitrum' AS blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
FROM
    {{ ref('silver__comp_withdraws') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) - INTERVAL '36 hours'
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    itoken AS protocol_market,
    received_contract_address AS token_address,
    received_contract_symbol token_symbol,
    amount_unadj,
    received_amount,
    redeemer AS depositor_address,
    platform,
    'arbitrum' AS blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
FROM
    {{ ref('silver__lodestar_withdraws') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) - INTERVAL '36 hours'
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    silo_market AS protocol_market,
    token_address,
    token_symbol,
    amount_unadj,
    amount,
    depositor_address,
    platform,
    'ethereum' AS blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
FROM
    {{ ref('silver__silo_withdraws') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) - INTERVAL '36 hours'
        FROM
            {{ this }}
    )
{% endif %}
)
SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    a.contract_address,
    CASE 
      WHEN platform = 'Fraxlend' THEN 'RemoveCollateral'
      WHEN platform = 'Compound V3' THEN 'WithdrawCollateral'
      WHEN platform = 'Compound V2' THEN 'Redeem'
      WHEN platform = 'Aave V1' THEN 'RedeemUnderlying'
      ELSE 'Withdraw'
    END AS event_name,
    protocol_market,
    depositor_address as depositor,
    a.token_address,
    a.token_symbol,
    amount_unadj,
    amount,
    ROUND(amount * price,2) AS amount_usd,
    platform,
    blockchain,
    a._log_id,
    a._inserted_timestamp
FROM
    withdraws a
LEFT JOIN {{ ref('price__ez_hourly_token_prices') }} p
ON a.token_address = p.token_address
AND DATE_TRUNC(
    'hour',
    block_timestamp
) = p.hour
LEFT JOIN {{ ref('silver__contracts') }} C
ON a.token_address = C.contract_address