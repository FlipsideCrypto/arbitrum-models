{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'product_id',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}
with decoded_logs as (
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        contract_address,
        event_name,
        decoded_log as decoded_log,
        topic_0,
        decoded_log :fundingRateEpochDuration :: INTEGER AS funding_rate_epoch_duration,
        decoded_log :fundingRateWindowTime :: INTEGER AS funding_rate_window_time,
        decoded_log :maxLeverage :: DECIMAL(
            38,
            0
        ) / 1e18 AS max_leverage,
        decoded_log :minAcceptablePortionLF :: DECIMAL(
            38,
            0
        ) / 1e18 AS min_acceptable_portion_lf,
        decoded_log :minAcceptableQuoteValue :: DECIMAL(
            38,
            0
        ) / 1e18 AS min_acceptable_quote_value,
        decoded_log :name :: STRING AS NAME,
        decoded_log :id :: STRING AS id,
        decoded_log :symbolId :: STRING AS symbol_id,
        decoded_log :tradingFee :: DECIMAL(
            38,
            0
        ) / 1e18 AS trading_fee,
        modified_timestamp,
        ez_decoded_event_logs_id
    FROM
        arbitrum.core.ez_decoded_event_logs --switch to ref once dev refresh is done
    WHERE
        contract_address = LOWER('0x8f06459f184553e5d04f07f868720bdacab39395')
        AND topic_0 = '0xf688a4e13cb28e5c12cf045718e469d8f189222cc94e8ffd4225f0703da46a72' --add_product
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
    tx_hash,
    block_number,
    block_timestamp,
    event_name,
    coalesce(id,symbol_id)::INTEGER as product_id,
    topic_0,
    funding_rate_epoch_duration,
    funding_rate_window_time,
    max_leverage,
    min_acceptable_portion_lf,
    min_acceptable_quote_value,
    NAME as product_name,
    trading_fee,
    modified_timestamp as _inserted_timestamp,
    ez_decoded_event_logs_id,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash','product_id']
    ) }} AS pear_products_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    decoded_logs qualify(ROW_NUMBER() over(PARTITION BY product_id
ORDER BY
    _inserted_timestamp DESC)) = 1