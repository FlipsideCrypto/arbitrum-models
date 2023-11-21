{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = ['block_number','platform'],
  cluster_by = ['block_timestamp::DATE'],
  tags = ['non_realtime','reorg','curated']
) }}

WITH repayments AS (

  SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    aave_market AS token_address,
    aave_token AS protocol_market,
    amount_unadj,
    repayed_tokens AS amount,
    symbol AS token_symbol,
    payer AS payer_address,
    borrower,
    'Aave V3' AS platform,
    'arbitrum' AS blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__aave_repayments') }}

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '36 hours'
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
    radiant_market AS token_address,
    radiant_token AS protocol_market,
    amount_unadj,
    repayed_tokens AS amount,
    symbol AS token_symbol,
    payer AS payer_address,
    borrower,
    'Radiant' AS platform,
    'arbitrum' AS blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__radiant_repayments') }}

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '36 hours'
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
    repay_contract_address AS token_address,
    itoken AS protocol_market,
    amount_unadj,
    repayed_amount AS amount,
    repay_contract_symbol AS token_symbol,
    payer AS payer_address,
    borrower,
    platform,
    'arbitrum' AS blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__lodestar_repayments') }}

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '36 hours'
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
  token_address,
  compound_market AS protocol_market,
  amount_unadj,
  amount,
  token_symbol,
  repayer AS payer_address,
  borrower,
  compound_version AS platform,
  'arbitrum' AS blockchain,
  _LOG_ID,
  _INSERTED_TIMESTAMP
FROM
  {{ ref('silver__comp_repayments') }}

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '36 hours'
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
  token_address,
  silo_market AS protocol_market,
  amount_unadj,
  amount,
  token_symbol,
  NULL as payer_address,
  depositor_address as borrower,
  platform,
  'arbitrum' AS blockchain,
  _LOG_ID,
  _INSERTED_TIMESTAMP
FROM
  {{ ref('silver__silo_repayments') }}

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '36 hours'
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
    WHEN platform = 'Fraxlend' THEN 'RepayAsset'
    WHEN platform = 'Compound V3' THEN 'Supply'
    WHEN platform = 'Compound V2' THEN 'RepayBorrow'
    ELSE 'Repay'
  END AS event_name,
  protocol_market,
  payer_address as payer,
  borrower,
  a.token_address,
  a.token_symbol,
  amount_unadj,
  amount,
  ROUND(amount * price,2) AS amount_usd,
  platform,
  blockchain,
  a._LOG_ID,
  a._INSERTED_TIMESTAMP
FROM
  repayments a
LEFT JOIN {{ ref('price__ez_hourly_token_prices') }} p
ON a.token_address = p.token_address
AND DATE_TRUNC(
    'hour',
    block_timestamp
) = p.hour
LEFT JOIN {{ ref('silver__contracts') }} C
ON a.token_address = C.contract_address
