-- depends_on: {{ ref('silver__complete_token_prices') }}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['block_number','platform'],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['reorg','curated','heal']
) }}

WITH aave AS (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        borrower_address AS borrower,
        aave_token AS protocol_market,
        aave_market AS token_address,
        symbol AS token_symbol,
        amount_unadj,
        amount,
        platform,
        'arbitrum' AS blockchain,
        A._LOG_ID,
        A._INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__aave_borrows') }} A

{% if is_incremental() and 'aave' not in var('HEAL_MODELS') %}
WHERE
    A._inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) - INTERVAL '{{ var(' lookback ', ' 4 hours ') }}'
        FROM
            {{ this }}
    )
{% endif %}
),
radiant as (
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        borrower_address AS borrower,
        radiant_token AS protocol_market,
        radiant_market AS token_address,
        symbol AS token_symbol,
        amount_unadj,
        amount,
        platform,
        'arbitrum' AS blockchain,
        A._LOG_ID,
        A._INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__radiant_borrows') }} A

    {% if is_incremental() and 'radiant' not in var('HEAL_MODELS') %}
    WHERE
        A._inserted_timestamp >= (
            SELECT
                MAX(
                    _inserted_timestamp
                ) - INTERVAL '{{ var(' lookback ', ' 4 hours ') }}'
            FROM
                {{ this }}
        )
    {% endif %}
),
lodestar as (
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
        itoken AS protocol_market,
        borrows_contract_address AS token_address,
        borrows_contract_symbol AS token_symbol,
        amount_unadj,
        loan_amount AS amount,
        platform,
        'arbitrum' AS blockchain,
        A._LOG_ID,
        A._INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__lodestar_borrows') }} A

    {% if is_incremental() and 'lodestar' not in var('HEAL_MODELS') %}
    WHERE
        A._inserted_timestamp >= (
            SELECT
                MAX(
                    _inserted_timestamp
                ) - INTERVAL '{{ var(' lookback ', ' 4 hours ') }}'
            FROM
                {{ this }}
        )
    {% endif %}
),
dforce as (
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
        token_address AS protocol_market,
        borrows_contract_address AS token_address,
        borrows_contract_symbol AS token_symbol,
        amount_unadj,
        amount,
        platform,
        'arbitrum' AS blockchain,
        A._LOG_ID,
        A._INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__dforce_borrows') }} A

{% if is_incremental() and 'dforce' not in var('HEAL_MODELS') %}
    WHERE
        A._inserted_timestamp >= (
            SELECT
                MAX(
                    _inserted_timestamp
                ) - INTERVAL '{{ var(' lookback ', ' 4 hours ') }}'
            FROM
                {{ this }}
        )
    {% endif %}
),
comp as (
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
        amount_unadj,
        amount,
        compound_version AS platform,
        'arbitrum' AS blockchain,
        l._LOG_ID,
        l._INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__comp_borrows') }}
        l

    {% if is_incremental() and 'comp' not in var('HEAL_MODELS') %}
    WHERE
        l._inserted_timestamp >= (
            SELECT
                MAX(
                    _inserted_timestamp
                ) - INTERVAL '{{ var(' lookback ', ' 4 hours ') }}'
            FROM
                {{ this }}
        )
    {% endif %}
),
silo as (
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
        amount_unadj,
        amount,
        platform,
        'arbitrum' AS blockchain,
        l._LOG_ID,
        l._INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__silo_borrows') }}
        l

    {% if is_incremental() and 'silo' not in var('HEAL_MODELS') %}
    WHERE
        l._inserted_timestamp >= (
            SELECT
                MAX(
                    _inserted_timestamp
                ) - INTERVAL '{{ var(' lookback ', ' 4 hours ') }}'
            FROM
                {{ this }}
        )
    {% endif %}
),
borrow_union as (
  SELECT
    *
  FROM
    aave
  UNION ALL
  SELECT
    *
  FROM
    radiant
  UNION ALL
  SELECT
    *
  FROM
    comp
  UNION ALL
  SELECT
    *
  FROM
    dforce
  UNION ALL
  SELECT
    *
  FROM
    lodestar
  UNION ALL
  SELECT
    *
  FROM
    silo
),
FINAL AS (
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
        amount_unadj,
        amount,
        ROUND(
            amount * price,
            2
        ) AS amount_usd,
        platform,
        b.blockchain,
        b._LOG_ID,
        b._INSERTED_TIMESTAMP
    FROM
        borrow_union b
        LEFT JOIN {{ ref('price__ez_prices_hourly') }}
        p
        ON b.token_address = p.token_address
        AND DATE_TRUNC(
            'hour',
            block_timestamp
        ) = p.hour
        LEFT JOIN {{ ref('silver__contracts') }} C
        ON b.token_address = C.contract_address
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash','event_index']
    ) }} AS complete_lending_borrows_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
