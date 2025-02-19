{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['tx_hash', 'quote_id'],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

with decoded_logs as (
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        origin_from_address,
        origin_to_address,
        contract_address,
        event_name,
        decoded_log,
        topic_0,
        decoded_log:cva::decimal(38,0)/1e18 as cva,
        decoded_log:deadline::integer as deadline,
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
        decoded_log:quoteId::integer as quote_id,
        decoded_log:symbolId::integer as symbol_id,
        decoded_log:tradingFee::decimal(38,0)/1e18 as trading_fee,
        modified_timestamp,
        ez_decoded_event_logs_id
    FROM
        arbitrum.core.ez_decoded_event_logs
    WHERE
        contract_address = LOWER('0x8f06459f184553e5d04f07f868720bdacab39395')
        {# AND origin_to_address = LOWER('0x6273242a7e88b3de90822b31648c212215caafe4') #}
        AND topic_0 = '0x8a17f103c77224ce4d9bab74dad3bd002cd24cf88d2e191e86d18272c8f135dd'
    {% if is_incremental() %}
    AND modified_timestamp >= (
        SELECT
            MAX(modified_timestamp) - INTERVAL '12 hours'
        FROM
            {{ this }}
    )
    AND modified_timestamp >= SYSDATE() - INTERVAL '7 day'
    {% endif %}
)
select 
    l.tx_hash,
    l.block_number,
    l.block_timestamp,
    l.event_name,
    l.origin_from_address,
    l.origin_to_address,
    l.symbol_id as product_id,
    l.topic_0,
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
    l.quote_id,
    l.trading_fee,
    p.product_name,
    l.modified_timestamp as _inserted_timestamp,
    l.ez_decoded_event_logs_id,
    {{ dbt_utils.generate_surrogate_key(
        ['l.tx_hash', 'l.quote_id']
    ) }} AS pear_perps_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    decoded_logs l
    LEFT JOIN {{ ref('silver__pear_dim_products') }} p
        ON l.symbol_id = p.product_id
qualify(ROW_NUMBER() over(PARTITION BY l.quote_id
ORDER BY
    _inserted_timestamp DESC)) = 1