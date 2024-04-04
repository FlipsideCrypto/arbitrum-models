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
