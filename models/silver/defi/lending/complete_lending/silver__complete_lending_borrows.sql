{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['block_number','platform'],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['non_realtime','reorg','curated']
) }}

with borrow_union AS (
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        borrower_address as borrower,
        aave_token AS protocol_market,
        aave_market AS token_address,
        symbol as token_symbol,
        borrowed_tokens AS amount,
        platform,
        blockchain,
        A._LOG_ID,
        A._INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__aave_borrows') }} A 
{% if is_incremental() %}
WHERE
    A._inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) - INTERVAL '36 hours'
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        borrower_address as borrower,
        radiant_token AS protocol_market,
        radiant_market AS token_address,
        symbol as token_symbol,
        borrowed_tokens AS amount,
        platform,
        blockchain,
        A._LOG_ID,
        A._INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__radiant_borrows') }} A 

{% if is_incremental() %}
WHERE
    A._inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) - INTERVAL '36 hours'
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    borrower,
    compound_market AS protocol_market,
    token_address,
    token_symbol,
    amount,
    compound_version AS platform,
    'ethereum' AS blockchain,
    l._LOG_ID,
    l._INSERTED_TIMESTAMP
FROM
    {{ ref('silver__comp_borrows') }}
    l

{% if is_incremental() %}
WHERE
    l._inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) - INTERVAL '12 hours'
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    borrower,
    silo_market AS protocol_market,
    token_address,
    token_symbol,
    amount,
    platform,
    'ethereum' AS blockchain,
    l._LOG_ID,
    l._INSERTED_TIMESTAMP
FROM
    {{ ref('silver__silo_borrows') }}
    l

{% if is_incremental() %}
WHERE
    l._inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) - INTERVAL '12 hours'
        FROM
            {{ this }}
    )
{% endif %}
)
SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    b.contract_address,
    CASE
        WHEN platform = 'Compound V3' THEN 'Withdraw'
        ELSE 'Borrow'
    END AS event_name,
    borrower,
    protocol_market,
    b.token_address,
    b.token_symbol,
    amount,
    ROUND(amount * price,2) AS amount_usd,
    platform,
    blockchain,
    b._LOG_ID,
    b._INSERTED_TIMESTAMP
FROM
    borrow_union b
LEFT JOIN {{ ref('price__fact_hourly_token_prices') }} p
ON b.token_address = p.token_address
AND DATE_TRUNC(
    'hour',
    block_timestamp
) = p.hour
LEFT JOIN {{ ref('silver__contracts') }} C
ON b.token_address = C.contract_address