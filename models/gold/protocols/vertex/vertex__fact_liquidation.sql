 {{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'VERTEX',
                'PURPOSE': 'DEX, LIQUIDATION'
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
    digest,
    trader,
    subaccount,
    MODE,
    health_group,
    amount_unadj,
    amount,
    amount_quote_unadj,
    amount_quote,
    insurance_cover_unadj,
    insurance_cover,
    _log_id,
    _inserted_timestamp,
    COALESCE (
        vertex_liquidation_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_hash', 'event_index']
        ) }}
    ) AS ez_vertex_perps_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__vertex_liquidations') }}