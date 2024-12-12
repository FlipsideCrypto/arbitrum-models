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
        --v1 staking contract
        (
            to_address = LOWER('0x5Be754aD77766089c4284d914F0cC37E8E3F669A')
            OR from_address = LOWER('0x5Be754aD77766089c4284d914F0cC37E8E3F669A')
        )
        --v2 staking contract
        OR (
            contract_address = LOWER('0x95146881b86B3ee99e63705eC87AfE29Fcc044D9')
            AND (
                to_address = LOWER('0x6e89C20F182b1744405603958eC5E3fd93441cc4')
                OR from_address = LOWER('0x6e89C20F182b1744405603958eC5E3fd93441cc4')
            )
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
            WHEN to_address = LOWER('0x6e89C20F182b1744405603958eC5E3fd93441cc4')
            AND from_address = LOWER('0x5Be754aD77766089c4284d914F0cC37E8E3F669A') THEN 'migrate-stake'
            WHEN to_address = LOWER('0x6e89C20F182b1744405603958eC5E3fd93441cc4') THEN 'stake'
            ELSE 'withdraw/claim'
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
