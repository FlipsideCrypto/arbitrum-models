{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = "block_number",
  cluster_by = ['block_timestamp::DATE'],
    tags = ['silver','defi','lending','curated']
) }}
-- pull all itoken addresses and corresponding name
WITH asset_details AS (

  SELECT
    itoken_address,
    itoken_symbol,
    itoken_name,
    itoken_decimals,
    underlying_asset_address,
    underlying_name,
    underlying_symbol,
    underlying_decimals
  FROM
    {{ ref('silver__lodestar_asset_details') }}
),
lodestar_repayments AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
    CONCAT('0x', SUBSTR(segmented_data [1] :: STRING, 25, 40)) AS borrower,
    contract_address AS itoken,
    CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 25, 40)) AS payer,
    utils.udf_hex_to_int(
      segmented_data [2] :: STRING
    ) :: INTEGER AS repayed_amount_raw,
    'Lodestar' AS platform,
    modified_timestamp AS _inserted_timestamp,
    CONCAT(
      tx_hash :: STRING,
      '-',
      event_index :: STRING
    ) AS _log_id
  FROM
    {{ ref('core__fact_event_logs') }}
  WHERE
    contract_address IN (
      SELECT
        itoken_address
      FROM
        asset_details
    )
    AND topics [0] :: STRING = '0x6fadbf7329d21f278e724fa0d4511001a158f2a97ee35c5bc4cf8b64417399ef'
    AND tx_succeeded

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
lodestar_combine AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    borrower,
    itoken,
    C.itoken_symbol,
    payer,
    repayed_amount_raw,
    C.underlying_asset_address AS repay_contract_address,
    C.underlying_symbol AS repay_contract_symbol,
    C.underlying_decimals,
    b.platform,
    b._log_id,
    b._inserted_timestamp
  FROM
    lodestar_repayments b
    LEFT JOIN asset_details C
    ON b.itoken = C.itoken_address
)
SELECT
  block_number,
  block_timestamp,
  tx_hash,
  event_index,
  origin_from_address,
  origin_to_address,
  origin_function_signature,
  contract_address,
  borrower,
  itoken,
  itoken_symbol,
  payer,
  repay_contract_address,
  repay_contract_symbol,
  repayed_amount_raw AS amount_unadj,
  repayed_amount_raw / pow(
    10,
    underlying_decimals
  ) AS repayed_amount,
  platform,
  _inserted_timestamp,
  _log_id
FROM
  lodestar_combine qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
  _inserted_timestamp DESC)) = 1
