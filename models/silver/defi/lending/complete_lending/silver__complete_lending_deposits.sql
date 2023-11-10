{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = ['block_number','platform'],
  cluster_by = ['block_timestamp::DATE'],
  tags = ['non_realtime','reorg','curated']
) }}

WITH deposits AS (

  SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    depositor_address,
    aave_token AS protocol_market,
    aave_market AS token_address,
    symbol as token_symbol,
    issued_tokens AS amount,
    platform,
    blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__aave_deposits') }}

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
    depositor_address,
    radiant_token AS protocol_market,
    radiant_market AS token_address,
    symbol as token_symbol,
    issued_tokens AS amount,
    platform,
    blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__radiant_deposits') }}

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
  depositor_address,
  compound_market AS protocol_market,
  token_address,
  token_symbol,
  amount,
  compound_version AS platform,
  'ethereum' AS blockchain,
  _LOG_ID,
  _INSERTED_TIMESTAMP
FROM
  {{ ref('silver__comp_deposits') }}

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
  depositor_address,
  silo_market AS protocol_market,
  token_address,
  token_symbol,
  amount,
  platform,
  'ethereum' AS blockchain,
  _LOG_ID,
  _INSERTED_TIMESTAMP
FROM
  {{ ref('silver__silo_deposits') }}

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
    WHEN platform = 'Fraxlend' THEN 'AddCollateral'
    WHEN platform = 'Compound V3' THEN 'SupplyCollateral'
    WHEN platform in ('Spark','Aave V3') THEN 'Supply'
    ELSE 'Deposit'
  END AS event_name,
  protocol_market,
  a.token_address,
  a.token_symbol,
  amount,
  ROUND(amount * price,2) AS amount_usd,
  platform,
  blockchain,
  a._LOG_ID,
  a._INSERTED_TIMESTAMP
FROM
  deposits a
LEFT JOIN {{ ref('price__fact_hourly_token_prices') }} p
ON a.token_address = p.token_address
AND DATE_TRUNC(
    'hour',
    block_timestamp
) = p.hour
LEFT JOIN {{ ref('silver__contracts') }} C
ON a.token_address = C.contract_address