{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = ['block_number','platform'],
  cluster_by = ['block_timestamp::DATE'],
  tags = ['non_realtime','reorg','curated']
) }}

WITH flashloans AS (

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
    aave_token AS protocol_token,
    amount_unadj,
    flashloan_amount,
    initiator_address,
    target_address,
    'Aave V3' AS platform,
    symbol,
    blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__aave_flashloans') }}

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
  radiant_token AS protocol_token,
  amount_unadj,
  flashloan_amount,
  initiator_address,
  target_address,
  platform,
  symbol AS token_symbol,
  blockchain,
  _LOG_ID,
  _INSERTED_TIMESTAMP
FROM
  {{ ref('silver__radiant_flashloans') }}

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
  f.contract_address,
  'FlashLoan' AS event_name,
  protocol_token AS protocol_market,
  initiator_address AS initiator,
  target_address AS target,
  f.token_address AS flashloan_token,
  token_symbol AS flashloan_token_symbol,
  amount_unadj AS flashloan_amount_unadj,
  flashloan_amount,
  ROUND(
    flashloan_amount * price,
    2
  ) AS flashloan_amount_usd,
  platform,
  blockchain,
  f._LOG_ID,
  f._INSERTED_TIMESTAMP
FROM
  flashloans f
  LEFT JOIN {{ ref('price__ez_hourly_token_prices') }}
  p
  ON f.token_address = p.token_address
  AND DATE_TRUNC(
    'hour',
    block_timestamp
  ) = p.hour
  LEFT JOIN {{ ref('silver__contracts') }} C
  ON f.token_address = C.contract_address 
