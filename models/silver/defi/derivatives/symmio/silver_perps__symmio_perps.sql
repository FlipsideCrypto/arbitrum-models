{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['_log_id', 'quote_id'],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH symmio_events AS (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        contract_address,
        event_name,
        event_index,
        origin_function_signature,
        decoded_log,
        topic_0,
        CONCAT(
            tx_hash,
            '-',
            event_index
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp,
        ez_decoded_event_logs_id
    FROM
        {{ ref('core__ez_decoded_event_logs') }}
    WHERE
        contract_address = LOWER('0x8f06459f184553e5d04f07f868720bdacab39395')
        AND topic_0 IN (
            '0x8a17f103c77224ce4d9bab74dad3bd002cd24cf88d2e191e86d18272c8f135dd',
            -- SendQuote
            '0xa50f98254710514f60327a4e909cd0be099a62f316299907ef997f3dc4d1cda5',
            -- OpenPosition
            '0x7f9710cf0d5a0ad968b7fc45b62e78bf71c0ca8ebb71a16128fc27b07fa5608d',
            -- RequestToClosePosition
            '0xfa7483d69b899cf16df47cc736ab853f88135f704980d7d358a9746aead7a321' -- FillCloseRequest
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
send_quotes AS (
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        contract_address,
        event_name,
        event_index,
        origin_function_signature,
        decoded_log,
        topic_0,
        TRY_TO_NUMBER(decoded_log:cva::STRING) / pow(10, 18) AS cva,
        TO_TIMESTAMP_NTZ(decoded_log:deadline::integer) AS deadline,
        TRY_TO_NUMBER(decoded_log:lf::STRING) / pow(10, 18) AS lf,
        TRY_TO_NUMBER(decoded_log:marketPrice::STRING) / pow(10, 18) AS market_price,
        decoded_log:orderType::integer as order_type,
        decoded_log:partyA::string as party_a,
        TRY_TO_NUMBER(decoded_log:partyAmm::STRING) / pow(10, 18) as party_amm,
        TRY_TO_NUMBER(decoded_log:partyBmm::STRING) / pow(10, 18) as party_bmm,
        decoded_log:partyBsWhiteList as party_bs_white_list,
        decoded_log:positionType::integer as position_type,
        TRY_TO_NUMBER(decoded_log:price::STRING) / pow(10, 18) as price,
        TRY_TO_NUMBER(decoded_log:quantity::STRING) / pow(10, 18) as quantity,
        decoded_log:quoteId::string as quote_id,
        decoded_log:symbolId::integer as symbol_id,
        TRY_TO_NUMBER(decoded_log:tradingFee::STRING) / pow(10, 18) as trading_fee,
        _log_id,
        _inserted_timestamp,
        ez_decoded_event_logs_id
    FROM
        symmio_events
    WHERE
        topic_0 = '0x8a17f103c77224ce4d9bab74dad3bd002cd24cf88d2e191e86d18272c8f135dd'
),
open_positions AS (
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        contract_address,
        event_name,
        event_index,
        origin_function_signature,
        decoded_log,
        topic_0,
        TRY_TO_NUMBER(decoded_log:filledAmount::STRING) / pow(10, 18) AS filled_amount,
        TRY_TO_NUMBER(decoded_log:openedPrice::STRING) / pow(10, 18) AS opened_price,
        decoded_log :partyA :: STRING AS party_a,
        decoded_log :partyB :: STRING AS party_b,
        decoded_log :quoteId :: STRING AS quote_id,
        _log_id,
        _inserted_timestamp,
        ez_decoded_event_logs_id
    FROM
        symmio_events
    WHERE
        topic_0 = '0xa50f98254710514f60327a4e909cd0be099a62f316299907ef997f3dc4d1cda5'
),
quote_status AS (
    SELECT
        l.tx_hash,
        l.block_number,
        l.block_timestamp,
        l.origin_from_address,
        l.origin_to_address,
        l.contract_address,
        l.event_name,
        l.event_index,
        l.origin_function_signature,
        l.decoded_log,
        l.topic_0,
        l.symbol_id AS product_id,
        CASE
            WHEN o.filled_amount IS NOT NULL THEN 'filled'
            ELSE 'unfilled'
        END AS status,
        l.cva,
        l.deadline,
        l.lf,
        l.market_price,
        l.order_type,
        l.party_a,
        l.party_amm,
        l.party_bmm,
        l.party_bs_white_list,
        l.position_type,
        l.price,
        l.quantity,
        o.filled_amount,
        o.opened_price,
        l.quote_id,
        l.trading_fee,
        p.product_name,
        l._log_id,
        l._inserted_timestamp,
        l.ez_decoded_event_logs_id
    FROM
        send_quotes l
        LEFT JOIN {{ ref('silver_perps__symmio_dim_products') }}
        p
        ON l.symbol_id = p.product_id
        LEFT JOIN open_positions o
        ON l.quote_id = o.quote_id

{% if is_incremental() %}
UNION ALL
SELECT
    l.tx_hash,
    l.block_number,
    l.block_timestamp,
    l.origin_from_address,
    l.origin_to_address,
    l.contract_address,
    l.event_name,
    l.event_index,
    l.origin_function_signature,
    l.decoded_log,
    l.topic_0,
    l.product_id,
    CASE
        WHEN o.filled_amount IS NOT NULL THEN 'filled'
        ELSE 'unfilled'
    END AS status,
    l.cva,
    l.deadline,
    l.lf,
    l.market_price,
    l.order_type,
    l.party_a,
    l.party_amm,
    l.party_bmm,
    l.party_bs_white_list,
    l.position_type,
    l.price,
    l.quantity,
    o.filled_amount,
    o.opened_price,
    l.quote_id,
    l.trading_fee,
    l.product_name,
    l._inserted_timestamp,
    l.ez_decoded_event_logs_id
FROM
    {{ this }}
    l
    LEFT JOIN open_positions o
    ON l.quote_id = o.quote_id
WHERE
    l.status = 'unfilled'
    AND l.event_name = 'SendQuote'
{% endif %}
),
request_close_position AS (
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        contract_address,
        event_name,
        event_index,
        origin_function_signature,
        decoded_log,
        topic_0,
        decoded_log :closeId :: INTEGER AS close_id,
        TRY_TO_NUMBER(decoded_log:closePrice::STRING) / pow(10, 18) AS close_price,
        TO_TIMESTAMP_NTZ(
            decoded_log :deadline :: INTEGER
        ) AS deadline,
        decoded_log :orderType :: INTEGER AS order_type,
        decoded_log :partyA :: STRING AS party_a,
        decoded_log :partyB :: STRING AS party_b,
        TRY_TO_NUMBER(decoded_log:quantityToClose::STRING) / pow(10, 18) AS quantity_to_close,
        decoded_log :quoteId :: INTEGER AS quote_id,
        decoded_log :quoteStatus :: INTEGER AS quote_status,
        _log_id,
        _inserted_timestamp,
        ez_decoded_event_logs_id
    FROM
        symmio_events
    WHERE
        topic_0 = '0x7f9710cf0d5a0ad968b7fc45b62e78bf71c0ca8ebb71a16128fc27b07fa5608d' -- RequestToClosePosition
),
fill_close_position AS (
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        contract_address,
        event_name,
        event_index,
        origin_function_signature,
        decoded_log,
        topic_0,
        decoded_log :closeId :: INTEGER AS close_id,
        TRY_TO_NUMBER(decoded_log:closedPrice::STRING) / pow(10, 18) AS closed_price,
        TRY_TO_NUMBER(decoded_log:filledAmount::STRING) / pow(10, 18) AS filled_amount,
        decoded_log :partyA :: STRING AS party_a,
        decoded_log :partyB :: STRING AS party_b,
        decoded_log :quoteId :: INTEGER AS quote_id,
        decoded_log :quoteStatus :: INTEGER AS quote_status,
        _log_id,
        _inserted_timestamp,
        ez_decoded_event_logs_id
    FROM
        symmio_events
    WHERE
        topic_0 = '0xfa7483d69b899cf16df47cc736ab853f88135f704980d7d358a9746aead7a321' -- FillCloseRequest
),
close_status AS (
    SELECT
        COALESCE(
            f.tx_hash,
            r.tx_hash
        ) AS tx_hash,
        COALESCE(
            f.block_number,
            r.block_number
        ) AS block_number,
        COALESCE(
            f.block_timestamp,
            r.block_timestamp
        ) AS block_timestamp,
        COALESCE(
            f.origin_from_address,
            r.origin_from_address
        ) AS origin_from_address,
        COALESCE(
            f.origin_to_address,
            r.origin_to_address
        ) AS origin_to_address,
        COALESCE(
            f.contract_address,
            r.contract_address
        ) AS contract_address,
        COALESCE(
            f.event_name,
            r.event_name
        ) AS event_name,
        COALESCE(
            f.event_index,
            r.event_index
        ) AS event_index,
        COALESCE(
            f.origin_function_signature,
            r.origin_function_signature
        ) AS origin_function_signature,
        COALESCE(
            f.decoded_log,
            r.decoded_log
        ) AS decoded_log,
        COALESCE(
            f.topic_0,
            r.topic_0
        ) AS topic_0,
        q.product_id AS product_id,
        CASE
            WHEN f.filled_amount IS NOT NULL THEN 'filled'
            ELSE 'unfilled'
        END AS status,
        q.cva,
        r.deadline,
        q.lf,
        q.market_price,
        r.order_type,
        r.party_a,
        q.party_amm,
        q.party_bmm,
        q.party_bs_white_list,
        q.position_type,
        f.closed_price AS price,
        r.quantity_to_close AS quantity,
        f.filled_amount,
        f.closed_price,
        r.quote_id,
        q.trading_fee,
        q.product_name AS product_name,
        r._log_id,
        r._inserted_timestamp,
        r.ez_decoded_event_logs_id
    FROM
        request_close_position r
        LEFT JOIN quote_status q
        ON r.quote_id = q.quote_id
        LEFT JOIN fill_close_position f
        ON r.quote_id = f.quote_id
        AND r.close_id = f.close_id

{% if is_incremental() %}
UNION ALL
SELECT
    l.tx_hash,
    l.block_number,
    l.block_timestamp,
    l.origin_from_address,
    l.origin_to_address,
    l.contract_address,
    l.event_name,
    l.event_index,
    l.origin_function_signature,
    l.decoded_log,
    l.topic_0,
    l.product_id,
    CASE
        WHEN f.filled_amount IS NOT NULL THEN 'filled'
        ELSE 'unfilled'
    END AS status,
    l.cva,
    l.deadline,
    l.lf,
    l.market_price,
    l.order_type,
    l.party_a,
    l.party_amm,
    l.party_bmm,
    l.party_bs_white_list,
    l.position_type,
    l.price,
    l.quantity,
    f.filled_amount,
    f.closed_price,
    l.quote_id,
    l.trading_fee,
    l.product_name,
    l._log_id,
    l._inserted_timestamp,
    l.ez_decoded_event_logs_id
FROM
    {{ this }}
    l
    LEFT JOIN fill_close_position f
    ON l.quote_id = f.quote_id
WHERE
    l.status = 'unfilled'
{% endif %}
)
SELECT
    tx_hash,
    block_number,
    block_timestamp,
    origin_from_address,
    origin_to_address,
    contract_address,
    event_name,
    event_index,
    origin_function_signature,
    decoded_log,
    topic_0,
    product_id,
    status,
    cva,
    deadline,
    lf,
    market_price,
    order_type,
    party_a,
    party_amm,
    party_bmm,
    party_bs_white_list,
    position_type,
    'open' AS action_type,
    price,
    quantity,
    filled_amount,
    quote_id,
    trading_fee,
    product_name,
    _log_id,
    _inserted_timestamp,
    ez_decoded_event_logs_id,
    {{ dbt_utils.generate_surrogate_key(
        ['_log_id', 'quote_id']
    ) }} AS symmio_perps_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    quote_status
UNION ALL
SELECT
    tx_hash,
    block_number,
    block_timestamp,
    origin_from_address,
    origin_to_address,
    contract_address,
    event_name,
    event_index,
    origin_function_signature,
    decoded_log,
    topic_0,
    product_id,
    status,
    cva,
    deadline,
    lf,
    market_price,
    order_type,
    party_a,
    party_amm,
    party_bmm,
    party_bs_white_list,
    position_type,
    'close' AS action_type,
    price,
    quantity,
    filled_amount,
    quote_id,
    trading_fee,
    product_name,
    _log_id,
    _inserted_timestamp,
    ez_decoded_event_logs_id,
    {{ dbt_utils.generate_surrogate_key(
        ['_log_id', 'quote_id']
    ) }} AS symmio_perps_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    close_status
