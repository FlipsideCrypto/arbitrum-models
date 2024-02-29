 {{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'VERTEX',
                'PURPOSE': 'CLOB, DEX, LIQUIDATION'
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
    health_group_symbol,
    amount_unadj,
    amount,
    amount_quote_unadj,
    amount_quote,
    insurance_cover_unadj,
    insurance_cover,
    vertex_liquidation_id AS ez_liquidations_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__vertex_liquidations') }}