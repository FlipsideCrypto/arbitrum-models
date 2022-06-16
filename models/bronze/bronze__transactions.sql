{{ config (
    materialized = 'view'
) }}

SELECT
    record_id,
    tx_id,
    tx_block_index,
    offset_id,
    block_id,
    block_timestamp,
    network,
    chain_id,
    tx,
    ingested_at,
    _inserted_timestamp
FROM
    {{ source(
        'prod',
        'arbitrum_txs'
    ) }}
