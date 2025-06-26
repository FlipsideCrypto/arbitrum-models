{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH dispatch AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        event_index,
        contract_address,
        decoded_log:sender :: string AS src_bridge_token, 
        -- src bridge token address, not user address
        try_to_number(decoded_log:destination :: string) as destination,
        decoded_log:recipient :: string AS dst_bridge_token,
        -- dst bridge token address, not recipient address
        DATA,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp
    FROM
        {{ ref('core__ez_decoded_event_logs') }}
    WHERE
        contract_address = LOWER('0x979Ca5202784112f4738403dBec5D0F3B9daabB9') -- arb mailbox
        AND topic_0 = '0x769f711d20c679153d382254f59892613b58a97cc876b249134ac25c80f9c814'
        AND tx_succeeded

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
),
dispatch_id AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        decoded_log:messageId :: string AS messageId,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp
    FROM
        {{ ref('core__ez_decoded_event_logs') }}
    WHERE
        contract_address = LOWER('0x979Ca5202784112f4738403dBec5D0F3B9daabB9') -- arb mailbox
        AND topic_0 = '0x788dbc1b7152732178210e7f4d9d010ef016f9eafbe66786bd7169f56e0c353a'
        AND tx_succeeded

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
),
gas_payment AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        topic_1 AS messageId,
        TRY_TO_NUMBER(decoded_log:destinationDomain :: string) AS destinationDomain,
        TRY_TO_NUMBER(decoded_log:gasAmount :: string) AS gasAmount,
        TRY_TO_NUMBER(decoded_log:payment :: string) AS payment,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp
    FROM
        {{ ref('core__ez_decoded_event_logs') }}
    WHERE
        contract_address = LOWER('0x3b6044acd6767f017e99318AA6Ef93b7B06A5a22') -- arb gas paymaster
        AND topic_0 = '0x65695c3748edae85a24cc2c60b299b31f463050bc259150d2e5802ec8d11720a'
        AND tx_succeeded

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
),
sent_transfer_remote AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
         TRY_TO_NUMBER(decoded_log:destination :: string) as destination,
        CONCAT('0x', SUBSTR(topic_2, 27, 40)) AS recipient,  -- actual recipient
        TRY_TO_NUMBER(decoded_log:amount :: string) as amount,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp
    FROM
        {{ ref('core__ez_decoded_event_logs') }}
    WHERE
        topic_0 = '0xd229aacb94204188fe8042965fa6b269c62dc5818b21238779ab64bdd17efeec'
        AND tx_succeeded

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
),
token_transfer AS (
    -- this matches tx_hash with each token's burn tx. this works since each contract only handles 1 token, but can be replaced by a 1 contract read of contracts in the hyperlane_asset (hyperlane_asset contracts have a wrappedtoken function)
    SELECT
        tx_hash,
        contract_address AS token_address
    FROM
        {{ ref('core__ez_token_transfers') }}
    WHERE
        tx_hash IN (
            SELECT
                tx_hash
            FROM
                sent_transfer_remote
        )
        AND to_address = '0x0000000000000000000000000000000000000000'
)
SELECT
    block_number,
    block_timestamp,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    tx_hash,
    event_index,
    contract_address AS bridge_address,
    'Dispatch' AS event_name,
    recipient AS destination_chain_receiver,
    destination AS destination_chain_id,
    messageId :: STRING AS message_id,
    gasAmount AS gas_amount,
    payment,
    origin_from_address AS sender,
    recipient AS receiver,
    amount,
    token_address,
    'hyperlane' AS platform,
    _log_id,
    modified_timestamp
FROM
    dispatch
    INNER JOIN dispatch_id USING(tx_hash)
    INNER JOIN gas_payment USING(tx_hash)
    INNER JOIN token_transfer USING(tx_hash)
    INNER JOIN sent_transfer_remote USING(tx_hash)
