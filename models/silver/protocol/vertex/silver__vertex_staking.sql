{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH stake_pull AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        from_address,
        to_address,
        raw_amount_precise :: INT AS amount_unadj,
        amount,
        amount_usd,
        symbol,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__transfers') }}
    WHERE
        to_address = LOWER('0x5Be754aD77766089c4284d914F0cC37E8E3F669A')
        OR from_address = LOWER('0x5Be754aD77766089c4284d914F0cC37E8E3F669A')

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
),
withdraws AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        NULL AS origin_from_address,
        NULL AS origin_to_address,
        '0x95146881b86b3ee99e63705ec87afe29fcc044d9' AS contract_address,
        from_address,
        to_address,
        'withdraw-request' AS stake_action,
        utils.udf_hex_to_int(SUBSTR(input_data, 11)) :: STRING AS amount_unadj,
        IFNULL(
            utils.udf_hex_to_int(SUBSTR(input_data, 11)) / pow(
                10,
                18
            ),
            0
        ) AS amount,
        NULL AS amount_usd,
        'VRTX' AS symbol,
        CONCAT(
            tx_hash,
            '-0'
        ) AS _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__transactions') }}
    WHERE
        origin_function_signature = '0x2e1a7d4d'
        AND to_address = LOWER('0x5be754ad77766089c4284d914f0cc37e8e3f669a')
),
claim_n_stake AS(
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        NULL AS from_address,
        NULL AS to_address,
        'claim-and-stake' AS stake_action,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        utils.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) :: INT AS amount_unadj, 
        amount_unadj / pow(
            10,
            18
        ) AS amount,
        NULL AS amount_usd,
        'VRTX' AS symbol,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING = '0x74d92e1d8f977540f36672b1cc287b447008d91265ec3a4e7c7b7c37fdb8ba74'
        AND origin_function_signature = '0x3f0e84ad'
),
standard_stakes AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        from_address,
        to_address,
        CASE
            WHEN to_address = LOWER('0x5Be754aD77766089c4284d914F0cC37E8E3F669A') THEN 'stake'
            WHEN from_address = LOWER('0x5Be754aD77766089c4284d914F0cC37E8E3F669A') THEN 'withdraw/claim'
            ELSE NULL
        END AS stake_action,
        amount_unadj,
        amount,
        amount_usd,
        symbol,
        _log_id,
        _inserted_timestamp
    FROM
        stake_pull
    WHERE
        symbol IN (
            'USDC',
            'VRTX'
        )
        AND to_address <> from_address
),
prices AS (
  SELECT
    HOUR,
    token_address,
    price
  FROM
    {{ ref('price__ez_prices_hourly') }}
  WHERE
    token_address IN (
        LOWER('0x95146881b86B3ee99e63705eC87AfE29Fcc044D9'),
        LOWER('0xaf88d065e77c8cC2239327C5EDb3A432268e5831'),
        LOWER('0xff970a61a04b1ca14834a43f5de4533ebddb5cc8')
    )
),
UNIONS AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        from_address,
        to_address,
        stake_action,
        amount_unadj,
        amount,
        amount_usd,
        symbol,
        _log_id,
        _inserted_timestamp
    FROM
        standard_stakes
    UNION ALL
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        from_address,
        to_address,
        stake_action,
        amount_unadj,
        amount,
        amount_usd,
        symbol,
        _log_id,
        _inserted_timestamp
    FROM
        claim_n_stake
    UNION ALL
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        from_address,
        to_address,
        stake_action,
        amount_unadj,
        amount,
        amount_usd,
        symbol,
        _log_id,
        _inserted_timestamp
    FROM
        withdraws
 ),
FINAL AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        from_address,
        to_address,
        stake_action,
        amount_unadj,
        amount,
        amount * price AS amount0_usd,
        symbol,
        _log_id,
        _inserted_timestamp
    FROM
        UNIONS u 
    LEFT JOIN
        prices p
    ON
        u.contract_address = p.token_address
    AND DATE_TRUNC(
      'hour',
      block_timestamp
    ) = p.hour
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['_log_id']
    ) }} AS vertex_staking_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
