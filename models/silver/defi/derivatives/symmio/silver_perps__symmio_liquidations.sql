{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH sendquote AS (
    -- symm sendquote (not just pear)

    SELECT
        block_timestamp,
        tx_hash,
        -- decoded_log,
        DATA,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        TRY_TO_NUMBER(
            arbitrum.utils.udf_hex_to_int(
                segmented_data [1] :: STRING
            )
        ) AS quoteId,
        TRY_TO_NUMBER(
            arbitrum.utils.udf_hex_to_int(
                segmented_data [3] :: STRING
            )
        ) AS symbolId,
        arbitrum.utils.udf_hex_to_int(
            segmented_data [4] :: STRING
        ) AS positionType,
        TRY_TO_NUMBER(
            arbitrum.utils.udf_hex_to_int(
                segmented_data [5] :: STRING
            )
        ) AS orderNum,
        CASE
            WHEN orderNum = 1 THEN 'market'
            WHEN orderNum = 0 THEN 'limit'
        END AS orderType,
        CASE
            WHEN positionType = 1 THEN 'short'
            WHEN positionType = 0 THEN 'long'
            ELSE 'unlabelled'
        END AS position_name,
        CONCAT(
            tx_hash,
            '-',
            event_index
        ) AS _log_id,
        modified_timestamp as _inserted_timestamp
    FROM
        {{ ref('core__ez_decoded_event_logs') }}
    WHERE
        contract_address = LOWER('0x8f06459f184553e5d04f07f868720bdacab39395')
        AND topics [0] :: STRING = '0x8a17f103c77224ce4d9bab74dad3bd002cd24cf88d2e191e86d18272c8f135dd'
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
sort_liquidate AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_to_address,
        origin_from_address,
        origin_function_signature,
        contract_address,
        event_name,
        event_index,
        decoded_log :partyA :: STRING AS partyA,
        decoded_log :liquidationId :: STRING AS liquidationid,
        TRY_TO_NUMBER(f1.value::STRING) AS closeId,
        TRY_TO_NUMBER(f2.value::STRING) AS quoteId,
        TRY_TO_NUMBER(f3.value::STRING) AS liquidatedAmount_unadj,
        -- in token quantity
        liquidatedAmount_unadj * pow(
            10,
            -18
        ) AS liquidatedAmount,
        f1.index,
        CONCAT(
            tx_hash,
            '-',
            event_index
        ) AS _log_id,
        modified_timestamp
    FROM
        {{ ref('core__ez_decoded_event_logs') }},
        LATERAL FLATTEN(
            input => decoded_log :closeIds
        ) f1,
        LATERAL FLATTEN(
            input => decoded_log :quoteIds
        ) f2,
        LATERAL FLATTEN(
            input => decoded_log :liquidatedAmounts
        ) f3
    WHERE
        contract_address = LOWER('0x8f06459f184553e5d04f07f868720bdacab39395')
        AND topic_0 = '0x74f4f24eca3f72e3bb723d7b35a487d7f59901d51c2060820babc5f460c8f7ab' -- liquidatepositionpartyA
        AND f1.index = f2.index
        AND f2.index = f3.index
        AND tx_succeeded

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
),
setsymbolprice AS (
    SELECT
        block_timestamp,
        tx_hash,
        decoded_log,
        decoded_log :liquidator :: STRING AS liquidator,
        decoded_log :partyA :: STRING AS partyA,
        decoded_log :liquidationId :: STRING AS liquidationid,
        TRY_TO_NUMBER(f1.value::STRING) AS symbolId,
        TRY_TO_NUMBER(f2.value::STRING) * pow(
            10,
            -18
        ) AS price,
        f1.index
    FROM
        {{ ref('core__ez_decoded_event_logs') }},
        LATERAL FLATTEN(
            input => decoded_log :symbolIds
        ) f1,
        LATERAL FLATTEN(
            input => decoded_log :prices
        ) f2
    WHERE
        contract_address = LOWER('0x8f06459f184553e5d04f07f868720bdacab39395')
        AND topic_0 = '0x7f333ff255d30b1324be748744a89e79af239d52dc603165f5221f5b57c1aaf5' -- SetSymbolsPrices
        AND f1.index = f2.index
        AND tx_succeeded

{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND modified_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
),
combine AS (
    SELECT
        p.block_timestamp AS price_timestamp,
        p.tx_hash AS price_tx_hash,
        l.block_timestamp,
        l.tx_hash,
        l.block_number,
        liquidationid,
        liquidator,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        event_name,
        event_index,
        l.partyA,
        quoteId,
        closeId,
        orderType,
        positionType,
        position_name,
        liquidatedAmount_unadj,
        liquidatedAmount,
        price,
        q.symbolId,
        liquidatedAmount * price AS liquidatedAmount_usd,
        _log_id,
        _inserted_timestamp
    FROM
        sort_liquidate l
        LEFT JOIN sendquote q USING(quoteId)
        LEFT JOIN setsymbolprice p USING (
            liquidationid,
            symbolId
        )
)
SELECT
    C.block_number,
    C.block_timestamp,
    C.tx_hash,
    price_timestamp,
    price_tx_hash,
    C.contract_address,
    C.origin_from_address,
    C.origin_to_address,
    C.event_name,
    C.event_index,
    C.origin_function_signature,
    C.partyA,
    C.quoteId AS quote_id,
    C.orderType AS order_type,
    C.positionType AS position_type,
    C.position_name,
    closeId AS close_id,
    liquidator,
    liquidationid AS liquidation_id,
    liquidatedAmount_unadj AS liquidated_amount_unadj,
    liquidatedAmount AS liquidated_amount,
    price,
    product_name,
    symbolId AS symbol_id,
    liquidatedAmount_usd AS liquidated_amount_usd,
    C._log_id,
    C._inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['C.tx_hash','C.event_index']
    ) }} AS symmio_liquidation_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    combine C
    LEFT JOIN {{ ref('silver_perps__symmio_dim_products') }}
    ON product_id = symbol_id qualify(ROW_NUMBER() over(PARTITION BY C._log_id
ORDER BY
    C._inserted_timestamp DESC)) = 1
