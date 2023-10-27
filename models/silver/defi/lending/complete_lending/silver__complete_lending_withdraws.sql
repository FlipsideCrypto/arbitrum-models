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
        aave_token AS protocol_token,
        aave_market AS withdraw_asset,
        symbol,
        withdrawn_tokens AS withdraw_amount,
        withdrawn_usd AS withdraw_amount_usd,
        depositor_address,
        aave_version AS platform,
        blockchain,
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
            ) - INTERVAL '12 hours'
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
    ctoken AS protocol_token,
    CASE
        WHEN received_contract_symbol = 'ETH' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        ELSE received_contract_address
    END AS withdraw_asset,
    received_contract_symbol AS symbol,
    received_amount AS withdraw_amount,
    received_amount_usd AS withdraw_amount_usd,
    redeemer AS depositor_address,
    compound_version AS platform,
    'ethereum' AS blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
FROM
    {{ ref('silver__compv3_withdraws') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) - INTERVAL '12 hours'
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
    contract_address,
    CASE 
      WHEN platform = 'Fraxlend' THEN 'RemoveCollateral'
      WHEN platform = 'Compound V3' THEN 'WithdrawCollateral'
      WHEN platform = 'Compound V2' THEN 'Redeem'
      WHEN platform = 'Aave V1' THEN 'RedeemUnderlying'
      ELSE 'Withdraw'
    END AS event_name,
    protocol_token AS protocol_market,
    withdraw_asset,
    a.symbol AS withdraw_symbol,
    withdraw_amount,
    CASE
        WHEN platform IN ('Fraxlenmd','Spark') 
        THEN ROUND(withdraw_amount * p.price / pow(10,C.decimals),2)
        ELSE ROUND(withdraw_amount_usd,2) 
    END AS withdraw_amount_usd,
    depositor_address,
    platform,
    blockchain,
    a._log_id,
    a._inserted_timestamp
FROM
    withdraws a
LEFT JOIN {{ ref('core__fact_hourly_token_prices') }} p
ON withdraw_asset = p.token_address
AND DATE_TRUNC(
    'hour',
    block_timestamp
) = p.hour
LEFT JOIN {{ ref('silver__contracts') }} C
ON withdraw_asset = C.address
