 {{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'VERTEX',
                'PURPOSE': 'DEX, PERPS'
            }
        }
    }
) }}

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
    symbol,
    digest,
    trader,
    subaccount,
    trade_type,
    expiration_raw,
    expiration,
    nonce,
    isTaker,
    price_amount_unadj,
    price_amount,
    amount_unadj,
    amount,
    amount_usd,
    fee_amount_unadj,
    fee_amount,
    base_delta_unadj,
    base_delta,
    quote_delta_unadj,
    quote_delta,
    COALESCE (
        vertex_perps_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_hash', 'event_index']
        ) }}
    ) AS ez_vertex_perps_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__vertex_perps') }}