{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'TRADER JOE, WOOFI, GMX, KYBERSWAP, ZYBERSWAP, DODO, FRAX, CAMELOT, UNISWAP, SUSHI, CURVE, BALANCER',
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
  amount_in,
  amount_in_usd,
  amount_out,
  amount_out_usd,
  sender,
  tx_to,
  event_index,
  platform,
  token_in,
  token_out,
  symbol_in,
  symbol_out,
  _log_id
FROM {{ ref('silver_dex__complete_dex_swaps') }}