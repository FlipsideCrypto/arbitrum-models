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
        CASE
            WHEN origin_function_signature = '0xa694fc3a' THEN 'stake'
            WHEN origin_function_signature = '0x1d6ee8eb' THEN 'claim_usdc'
            WHEN origin_function_signature = '0x0d51a300' THEN 'claim_usdc_and_stake'
            WHEN origin_function_signature = '0x5de8c74d' THEN 'claim_vrtx'
            WHEN origin_function_signature = '0x2e1a7d4d' THEN 'withdraw'
            ELSE 'non_staking_tx'
        END AS function_name,
        raw_amount_precise AS amount_unadj,
        amount,
        amount_usd,
        symbol,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__transfers') }}
    WHERE
        origin_from_address = LOWER('0x5Be754aD77766089c4284d914F0cC37E8E3F669A')
        OR origin_to_address = LOWER('0x5Be754aD77766089c4284d914F0cC37E8E3F669A')

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
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash']
    ) }} AS vertex_spot_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    stake_pull 
WHERE
    function_name <> 'non_staking_tx' qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1