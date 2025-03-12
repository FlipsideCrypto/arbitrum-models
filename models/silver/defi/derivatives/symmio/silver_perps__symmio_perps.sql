{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['tx_hash', 'quote_id'],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

with symmio_events as (
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
        modified_timestamp as _inserted_timestamp,
        ez_decoded_event_logs_id
    FROM
        {{ ref('core__ez_decoded_event_logs') }}
    WHERE
        contract_address = LOWER('0x8f06459f184553e5d04f07f868720bdacab39395')
        AND topic_0 IN (
            '0x8a17f103c77224ce4d9bab74dad3bd002cd24cf88d2e191e86d18272c8f135dd', -- SendQuote
            '0xa50f98254710514f60327a4e909cd0be099a62f316299907ef997f3dc4d1cda5', -- OpenPosition
            '0x7f9710cf0d5a0ad968b7fc45b62e78bf71c0ca8ebb71a16128fc27b07fa5608d', -- RequestToClosePosition
            '0xfa7483d69b899cf16df47cc736ab853f88135f704980d7d358a9746aead7a321'  -- FillCloseRequest
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

send_quotes as (
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
        decoded_log:cva::decimal(38,0)/1e18 as cva,
        TO_TIMESTAMP_NTZ(decoded_log:deadline::integer) as deadline,
        decoded_log:lf::decimal(38,0)/1e18 as lf,
        decoded_log:marketPrice::decimal(38,0)/1e18 as market_price,
        decoded_log:orderType::integer as order_type,
        decoded_log:partyA::string as party_a,
        decoded_log:partyAmm::decimal(38,0)/1e18 as party_amm,
        decoded_log:partyBmm::decimal(38,0)/1e18 as party_bmm,
        decoded_log:partyBsWhiteList as party_bs_white_list,
        decoded_log:positionType::integer as position_type,
        decoded_log:price::decimal(38,0)/1e18 as price,
        decoded_log:quantity::decimal(38,0)/1e18 as quantity,
        decoded_log:quoteId::string as quote_id,
        decoded_log:symbolId::integer as symbol_id,
        decoded_log:tradingFee::decimal(38,0)/1e18 as trading_fee,
        _log_id,
        _inserted_timestamp,
        ez_decoded_event_logs_id
    FROM
        symmio_events
    WHERE
        topic_0 = '0x8a17f103c77224ce4d9bab74dad3bd002cd24cf88d2e191e86d18272c8f135dd'
),

open_positions as (
    select
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
        decoded_log:filledAmount::decimal(38,0)/1e18 as filled_amount,
        decoded_log:openedPrice::decimal(38,0)/1e18 as opened_price,
        decoded_log:partyA::string as party_a,
        decoded_log:partyB::string as party_b,
        decoded_log:quoteId::string as quote_id,
        _log_id,
        _inserted_timestamp,
        ez_decoded_event_logs_id
    FROM
        symmio_events
    WHERE
        topic_0 = '0xa50f98254710514f60327a4e909cd0be099a62f316299907ef997f3dc4d1cda5'
),

quote_status as (
    select 
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
        l.symbol_id as product_id,
        CASE 
            WHEN o.filled_amount is not null THEN 'filled' 
            ELSE 'unfilled' 
        END as status,
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
    LEFT JOIN {{ ref('silver_perps__symmio_dim_products') }} p
        ON l.symbol_id = p.product_id
    LEFT JOIN open_positions o
        ON l.quote_id = o.quote_id
    {% if is_incremental() %}
    UNION ALL
    select 
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
            WHEN o.filled_amount is not null THEN 'filled' 
            ELSE 'unfilled' 
        END as status,
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
        {{this}} l
    LEFT JOIN open_positions o
        ON l.quote_id = o.quote_id
    WHERE
        l.status = 'unfilled'
        and l.event_name ='SendQuote' 
    {% endif %}
),

request_close_position as (
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
        decoded_log:closeId::integer as close_id,
        decoded_log:closePrice::decimal(38,0)/1e18 as close_price,
        TO_TIMESTAMP_NTZ(decoded_log:deadline::integer) as deadline,
        decoded_log:orderType::integer as order_type,
        decoded_log:partyA::string as party_a,
        decoded_log:partyB::string as party_b,
        decoded_log:quantityToClose::decimal(38,0)/1e18 as quantity_to_close,
        decoded_log:quoteId::integer as quote_id,
        decoded_log:quoteStatus::integer as quote_status,
        _log_id,
        _inserted_timestamp,
        ez_decoded_event_logs_id
    FROM
        symmio_events
    WHERE
        topic_0 = '0x7f9710cf0d5a0ad968b7fc45b62e78bf71c0ca8ebb71a16128fc27b07fa5608d' -- RequestToClosePosition
),

fill_close_position as (
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
        decoded_log:closeId::integer as close_id,
        decoded_log:closedPrice::decimal(38,0)/1e18 as closed_price,
        decoded_log:filledAmount::decimal(38,0)/1e18 as filled_amount,
        decoded_log:partyA::string as party_a,
        decoded_log:partyB::string as party_b,
        decoded_log:quoteId::integer as quote_id,
        decoded_log:quoteStatus::integer as quote_status,
        _log_id,
        _inserted_timestamp,
        ez_decoded_event_logs_id
    FROM
        symmio_events
    WHERE
        topic_0 = '0xfa7483d69b899cf16df47cc736ab853f88135f704980d7d358a9746aead7a321' -- FillCloseRequest
),

close_status as (
    select 
        coalesce(f.tx_hash, r.tx_hash) as tx_hash,
        coalesce(f.block_number, r.block_number) as block_number,
        coalesce(f.block_timestamp, r.block_timestamp) as block_timestamp,
        coalesce(f.origin_from_address, r.origin_from_address) as origin_from_address,
        coalesce(f.origin_to_address, r.origin_to_address) as origin_to_address,
        coalesce(f.contract_address, r.contract_address) as contract_address,
        coalesce(f.event_name, r.event_name) as event_name,
        coalesce(f.event_index, r.event_index) as event_index,
        coalesce(f.origin_function_signature, r.origin_function_signature) as origin_function_signature,
        coalesce(f.decoded_log, r.decoded_log) as decoded_log,
        coalesce(f.topic_0, r.topic_0) as topic_0,
        q.product_id as product_id,
        CASE 
            WHEN f.filled_amount is not null THEN 'filled' 
            ELSE 'unfilled' 
        END as status,
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
        f.closed_price as price,
        r.quantity_to_close as quantity,
        f.filled_amount,
        f.closed_price,
        r.quote_id,
        q.trading_fee,
        q.product_name as product_name,
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
    select 
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
            WHEN f.filled_amount is not null THEN 'filled' 
            ELSE 'unfilled' 
        END as status,
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
        {{this}} l
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
    'open' as action_type,
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
        ['tx_hash', 'quote_id']
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
    'close' as action_type,
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
        ['tx_hash', 'quote_id']
    ) }} AS symmio_perps_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM 
    close_status