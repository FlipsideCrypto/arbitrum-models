{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH oft_asset_contract_creation AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        from_address AS oft_address,
        tx_succeeded,
        trace_succeeded,
        modified_timestamp
    FROM
        {{ ref('core__fact_traces') }}
    WHERE
        TYPE = 'CALL'
        AND LEFT(
            input,
            10
        ) = '0xca5eb5e1'
        AND to_address = '0x1a44076050125825900e736c501f859c50fe728c' -- layerzero v2
        AND tx_succeeded
        AND trace_succeeded

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
    FROM
        {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}

qualify ROW_NUMBER() over (
    PARTITION BY oft_address
    ORDER BY
        block_timestamp DESC
) = 1
),
oft_asset_base_token AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        from_address AS wrap_address,
        to_address AS underlying_address,
        tx_succeeded,
        trace_succeeded,
        modified_timestamp
    FROM
        {{ ref('core__fact_traces') }}
    WHERE
        tx_hash IN (
            SELECT
                tx_hash
            FROM
                oft_asset_contract_creation
        )
        AND TYPE = 'STATICCALL'
        AND input = '0x313ce567' qualify ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                trace_index ASC
        ) = 1
),
oft_asset AS (
    SELECT
        oft_address,
        underlying_address
    FROM
        oft_asset_contract_creation t1
        LEFT JOIN oft_asset_base_token t2
        ON t1.tx_hash = t2.tx_hash
        AND oft_address = wrap_address
),
oft_sent AS (
    -- bridging transactions
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        event_index,
        'OFTSent' AS event_name,
        'layerzero-v2' AS platform,
        oft_address,
        underlying_address,
        SUBSTR(
            topics [1] :: STRING,
            2,
            64
        ) AS guid,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS from_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data [0] :: STRING)) AS dstEid,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data [1] :: STRING)) AS amountSentLD,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data [2] :: STRING)) AS amountReceivedLD,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
        e
        INNER JOIN oft_asset
        ON oft_address = contract_address
    WHERE
        topics [0] = '0x85496b760a4b7f8d66384b9df21b381f5d1b1e79f229a47aaf4c232edc2fe59a'

{% if is_incremental() %}
AND e.modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
    FROM
        {{ this }}
)
AND e.modified_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    '0x1a44076050125825900e736c501f859c50fe728c' AS bridge_address,
    event_index,
    event_name,
    platform,
    from_address AS sender,
    from_address AS receiver,
    from_address AS destination_chain_receiver,
    amountSentLD AS amount,
    b.dstEid AS destination_chain_id,
    LOWER(
        s.chain :: STRING
    ) AS destination_chain,
    COALESCE(
        underlying_address,
        oft_address
    ) AS token_address,
    oft_address,
    _log_id,
    modified_timestamp
FROM
    oft_sent b
    INNER JOIN {{ ref('silver_bridge__layerzero_bridge_seed') }}
    s
    ON b.dstEid :: STRING = s.eid :: STRING
