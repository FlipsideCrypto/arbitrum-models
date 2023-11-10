{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = ['block_number','platform'],
  cluster_by = ['block_timestamp::DATE'],
  tags = ['non_realtime','reorg','curated']
) }}

WITH compv2_join AS (
  SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    absorber as  liquidator,
    borrower,
    liquidated_amount,
    liquidated_amount_usd,
    compound_market AS protocol_collateral_asset,
    token_address AS collateral_asset,
    token_symbol AS collateral_asset_symbol,
    debt_asset,
    debt_asset_symbol,
    l.compound_version AS platform,
    'ethereum' AS blockchain,
    l._LOG_ID,
    l._INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__comp_liquidations') }}
    l

{% if is_incremental() %}
WHERE
  l._inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
      {{ this }}
  )
{% endif %}
),
liquidation_union AS (
  SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    liquidator,
    borrower,
    liquidated_amount,
    liquidated_amount_usd,
    protocol_collateral_asset,
    collateral_asset,
    collateral_asset_symbol,
    debt_asset,
    debt_asset_symbol,
    platform,
    blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    compv2_join
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
    liquidator,
    borrower,
    liquidated_amount,
    NULL AS liquidated_amount_usd,
    collateral_aave_token AS protocol_collateral_asset,
    collateral_asset,
    collateral_token_symbol AS collateral_asset_symbol,
    debt_asset,
    debt_token_symbol AS debt_asset_symbol,
    'Aave V3' AS platform,
    blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__aave_liquidations') }}

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '12 hours'
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
    liquidator,
    borrower,
    liquidated_amount,
    NULL AS liquidated_amount_usd,
    collateral_radiant_token AS protocol_collateral_asset,
    collateral_asset,
    collateral_token_symbol AS collateral_asset_symbol,
    debt_asset,
    debt_token_symbol AS debt_asset_symbol,
    'Radiant' AS platform,
    blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__radiant_liquidations') }}

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
      {{ this }}
  )
{% endif %}
),
contracts as (
  SELECT
    *
  FROM
    {{ ref('silver__contracts') }} c
  where 
    c.contract_address in (
      SELECT distinct(collateral_asset) AS asset FROM liquidation_union
    )
),
prices as (
  SELECT
    *
  FROM
    {{ ref('price__fact_hourly_token_prices') }} p
  where 
    token_address in (
      SELECT distinct(collateral_asset) AS asset FROM liquidation_union
    )
  AND
    HOUR > (SELECT MIN(block_timestamp) FROM liquidation_union)

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
    WHEN platform = 'Fraxlend' THEN 'Liquidate'
    WHEN platform = 'Compound V3' THEN 'AbsorbCollateral'
    WHEN platform = 'Compound V2' THEN 'LiquidateBorrow'
    ELSE 'LiquidationCall'
  END AS event_name,
  liquidator,
  borrower,
  protocol_collateral_asset,
  collateral_asset,
  collateral_asset_symbol,
  liquidated_amount AS liquidation_amount,
  CASE
    WHEN platform <> 'Compound V3'
    THEN ROUND(liquidated_amount * p.price,2)
    ELSE ROUND(liquidated_amount_usd,2) 
    END AS liquidation_amount_usd,
  debt_asset,
  debt_asset_symbol,
  platform,
  a.blockchain,
  a._LOG_ID,
  a._INSERTED_TIMESTAMP
FROM
  liquidation_union a
LEFT JOIN prices p
ON collateral_asset = p.token_address
AND DATE_TRUNC(
  'hour',
  block_timestamp
) = p.hour
LEFT JOIN contracts C
ON collateral_asset = C.contract_address