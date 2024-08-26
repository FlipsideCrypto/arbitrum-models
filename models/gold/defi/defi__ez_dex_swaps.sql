{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'TRADER JOE, WOOFI, GMX, KYBERSWAP, ZYBERSWAP, DODO, FRAX, CAMELOT, UNISWAP, SUSHI, CURVE, BALANCER, HASHFLOW',
                'PURPOSE': 'DEX, SWAPS'
            }
        }
    }
) }}

SELECT
  block_number,
  block_timestamp,
  tx_hash,
  origin_function_signature,
  origin_from_address,
  origin_to_address,
  contract_address,
  pool_name,
  event_name,
  amount_in_unadj,
  amount_in,
  ROUND(
        CASE
            WHEN token_in <> '0x82af49447d8a07e3bd95bd0d56f35241523fbab1'
            AND (
                amount_out_usd IS NULL
                OR ABS((amount_in_usd - amount_out_usd) / NULLIF(amount_out_usd, 0)) > 0.75
                OR ABS((amount_in_usd - amount_out_usd) / NULLIF(amount_in_usd, 0)) > 0.75
            ) THEN NULL
            ELSE amount_in_usd
        END,
        2
    ) AS amount_in_usd,
    amount_out_unadj,
    amount_out,
    ROUND(
        CASE
            WHEN token_out <> '0x82af49447d8a07e3bd95bd0d56f35241523fbab1'
            AND (
                amount_in_usd IS NULL
                OR ABS((amount_out_usd - amount_in_usd) / NULLIF(amount_in_usd, 0)) > 0.75
                OR ABS((amount_out_usd - amount_in_usd) / NULLIF(amount_out_usd, 0)) > 0.75
            ) THEN NULL
            ELSE amount_out_usd
        END,
        2
    ) AS amount_out_usd,
  sender,
  tx_to,
  event_index,
  platform,
  token_in,
  token_out,
  symbol_in,
  symbol_out,
  _log_id,
    COALESCE (
        complete_dex_swaps_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_hash','event_index']
        ) }}
    ) AS ez_dex_swaps_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM {{ ref('silver_dex__complete_dex_swaps') }}