{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH stargate_contracts AS (
    SELECT
        block_timestamp,
        tx_hash,
        to_address AS contract_address,
        regexp_substr_all(SUBSTR(input, -704, 714), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(segmented_data [2], 25, 40)) AS token_address,
        utils.udf_hex_to_int(
            segmented_data [3] :: STRING
        ) AS token_decimals,
        segmented_data [4] AS shared_decimals,
        utils.udf_hex_to_string(
            segmented_data [8] :: STRING
        ) AS token_name,
        utils.udf_hex_to_string(
            segmented_data [10] :: STRING
        ) AS token_symbol
    FROM
        {{ ref('core__fact_traces') }}
    WHERE
        origin_function_signature = '0x61014060'
        AND from_address = '0x4a79adc4539905376d339c69b6a7092d0598cc24'
        AND trace_succeeded

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
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
        'stargate-v2' AS platform,
        contract_address,
        decoded_log :guid :: STRING AS guid,
        decoded_log :fromAddress :: STRING AS from_address,
        TRY_TO_NUMBER(
            decoded_log :dstEid :: STRING
        ) AS dstEid,
        TRY_TO_NUMBER(
            decoded_log :amountSentLD :: STRING
        ) AS amountSentLD,
        TRY_TO_NUMBER(
            decoded_log :amountReceivedLD :: STRING
        ) AS amountReceivedLD,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__ez_decoded_event_logs') }}
        e
        INNER JOIN stargate_contracts USING(contract_address)
    WHERE
        topics [0] = '0x85496b760a4b7f8d66384b9df21b381f5d1b1e79f229a47aaf4c232edc2fe59a'
        AND tx_succeeded

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
),
token_transfers AS (
    SELECT
        tx_hash,
        contract_address AS token_address,
        raw_amount AS amount_unadj
    FROM
        {{ ref('core__ez_token_transfers') }}
    WHERE
        to_address IN (
            SELECT
                contract_address
            FROM
                stargate_contracts
        )
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                oft_sent
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
),
-- only if there's native
native_transfers AS (
    SELECT
        tx_hash,
        '0x82aF49447D8a07e3bd95BD0d56f35241523fBab1' AS token_address,
        -- hard coded weth
        amount AS amount_unadj
    FROM
        {{ ref('core__ez_native_transfers') }}
    WHERE
        to_address IN (
            SELECT
                contract_address
            FROM
                stargate_contracts
        )
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                oft_sent
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
),
bus_mode AS (
    SELECT
        tx_hash,
        decoded_log :dstEid :: STRING AS dst_eid,
        decoded_log :fare :: STRING AS fare,
        decoded_log :passenger :: STRING AS passenger,
        TRY_TO_NUMBER(utils.udf_hex_to_int(SUBSTR(passenger, 3, 4))) AS asset_id,
        CONCAT('0x', SUBSTR(passenger, 3 + 4 + 24, 40)) AS receiver,
        TRY_TO_NUMBER(
            decoded_log :ticketId :: STRING
        ) AS ticketId
    FROM
        {{ ref('core__ez_decoded_event_logs') }}
    WHERE
        contract_address = LOWER('0x19cfce47ed54a88614648dc3f19a5980097007dd') -- tokenmessaging
        AND topics [0] = '0x15955c5a4cc61b8fbb05301bce47fd31c0e6f935e1ab97fdac9b134c887bb074' -- BusRode
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                oft_sent
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
),
taxi_mode AS (
    SELECT
        tx_hash,
        input,
        SUBSTR(input, 11, len(input)),
        regexp_substr_all(SUBSTR(input, 11, len(input)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(segmented_data [4] :: STRING, 25, 40)) AS sender,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data [5] :: STRING)) AS dstEid,
        CONCAT('0x', SUBSTR(segmented_data [6] :: STRING, 25, 40)) AS receiver,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data [7] :: STRING)) AS amountSD
    FROM
        {{ ref('core__fact_traces') }}
    WHERE
        to_address = '0x19cfce47ed54a88614648dc3f19a5980097007dd'
        AND from_address IN (
            SELECT
                contract_address
            FROM
                stargate_contracts
        )
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                oft_sent
        )
        AND LEFT(
            input,
            10
        ) = '0xff6fb300'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    b.tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    '0x1a44076050125825900e736c501f859c50fe728c' AS bridge_address,
    event_index,
    event_name,
    platform,
    origin_from_address AS sender,
    receiver,
    receiver AS destination_chain_receiver,
    amountSentLD AS amount_unadj,
    b.dstEid AS destination_chain_id,
    LOWER(
        s.chain :: STRING
    ) AS destination_chain,
    t.token_address,
    _log_id,
    b._inserted_timestamp
FROM
    oft_sent b
    INNER JOIN (
        SELECT
            *
        FROM
            token_transfers
        UNION ALL
        SELECT
            *
        FROM
            native_transfers
    ) t -- to get srctokens
    ON b.tx_hash = t.tx_hash
    AND b.amountSentLD = t.amount_unadj
    LEFT JOIN (
        SELECT
            receiver,
            tx_hash
        FROM
            bus_mode
        UNION ALL
        SELECT
            receiver,
            tx_hash
        FROM
            taxi_mode
    ) m
    ON m.tx_hash = b.tx_hash
    INNER JOIN blast.silver_bridge.layerzero_bridge_seed s
    ON b.dstEid :: STRING = s.eid :: STRING
ORDER BY
    block_timestamp DESC
