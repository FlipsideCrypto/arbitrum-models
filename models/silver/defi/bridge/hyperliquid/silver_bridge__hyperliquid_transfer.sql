{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH usdc_transfer AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        event_index,
        contract_address,
        from_address,
        to_address,
        raw_amount,
        symbol,
        amount,
        CONCAT(
            tx_hash,
            '-',
            event_index
        ) AS _log_id,
        modified_timestamp
    FROM
        arbitrum.core.ez_token_transfers
    WHERE
        contract_address IN (
            '0xaf88d065e77c8cc2239327c5edb3a432268e5831',
            '0xff970a61a04b1ca14834a43f5de4533ebddb5cc8'
        )
        AND to_address = '0x2df1c51e09aecf9cacb7bc98cb1742757f163df7'
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    event_index,
    'Deposit' AS event_name,
    to_address AS bridge_address,
    'hyperliquid' AS destination_chain,
    999 AS destination_chain_id,
    contract_address AS token_address,
    symbol AS token_symbol,
    raw_amount AS amount_unadj,
    amount,
    _log_id,
    t.modified_timestamp
FROM
    usdc_transfer t