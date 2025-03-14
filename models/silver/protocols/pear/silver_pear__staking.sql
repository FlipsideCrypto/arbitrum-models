{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH logs_pull AS (

    SELECT
        block_timestamp,
        tx_hash,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        topics,
        topic_0,
        topic_1,
        DATA,
        modified_timestamp AS _inserted_timestamp,
        CONCAT(
            tx_hash,
            '-',
            event_index
        ) AS _log_id
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        contract_address = '0xce3be5204017bb1bd279937f92df09fd7f539b92'
        AND topic_0 IN (
            '0x9e71bc8eea02a63969f509818f2dafb9254532904319f9dbda79b67bd34a5f3d',
            -- staked
            '0x7fc4727e062e336010f2c282598ef5f14facb3de68cf8195c2f23e1454b2b74e',
            -- unstaked
            '0xaaec67f0cf2b4350aec177973525b594b0ef343afc049ce70b0808e96a1b5e64',
            -- exitfeereward
            '0xd0738c40db6944b0431635619e5439399d30b1c3201de82a76281ad5e589a331',
            -- stakingreward
            '0x4139253da630b62761fa266dd3ef78ee3a1a13aa977ced7176f27084cba40265' -- compound
        )
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
unstaked AS (
    SELECT
        block_timestamp,
        tx_hash,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        topics,
        topic_0,
        DATA,
        'unstaked' AS action,
        CONCAT('0x', SUBSTR(topic_1, 27, 40)) AS user_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data [0] :: STRING)) * pow(
            10,
            -18
        ) AS amount,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data [1] :: STRING)) * pow(
            10,
            -18
        ) AS exitFee,
        'PEAR' AS reward_token,
        _inserted_timestamp,
        _log_id
    FROM
        logs_pull
    WHERE
        topic_0 = '0x7fc4727e062e336010f2c282598ef5f14facb3de68cf8195c2f23e1454b2b74e' -- unstaked
),
staked AS (
    SELECT
        block_timestamp,
        tx_hash,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        topics,
        topic_0,
        DATA,
        'staked' AS action,
        CONCAT('0x', SUBSTR(topic_1, 27, 40)) AS user_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data [0] :: STRING)) * pow(
            10,
            -18
        ) AS amount,
        0 AS exitFee,
        'PEAR' AS reward_token,
        _inserted_timestamp,
        _log_id
    FROM
        logs_pull
    WHERE
        topic_0 = '0x9e71bc8eea02a63969f509818f2dafb9254532904319f9dbda79b67bd34a5f3d' -- staked
),
claims AS (
    SELECT
        block_timestamp,
        tx_hash,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        topics,
        topic_0,
        DATA,
        'claim-withdraw' AS action,
        CONCAT('0x', SUBSTR(topic_1, 27, 40)) AS user_address,
        CASE
            WHEN topic_0 = '0xd0738c40db6944b0431635619e5439399d30b1c3201de82a76281ad5e589a331' THEN TRY_TO_NUMBER(utils.udf_hex_to_int(DATA :: STRING)) * pow(
                10,
                -18
            )
            WHEN topic_0 = '0xaaec67f0cf2b4350aec177973525b594b0ef343afc049ce70b0808e96a1b5e64' THEN TRY_TO_NUMBER(utils.udf_hex_to_int(DATA :: STRING)) * pow(
                10,
                -18
            )
        END AS amount,
        CASE
            WHEN topic_0 = '0xd0738c40db6944b0431635619e5439399d30b1c3201de82a76281ad5e589a331' THEN 'ETH'
            WHEN topic_0 = '0xaaec67f0cf2b4350aec177973525b594b0ef343afc049ce70b0808e96a1b5e64' THEN 'PEAR'
        END AS reward_token,
        _inserted_timestamp,
        _log_id
    FROM
        logs_pull
    WHERE
        topic_0 IN (
            '0xd0738c40db6944b0431635619e5439399d30b1c3201de82a76281ad5e589a331',
            -- stakingreward
            '0xaaec67f0cf2b4350aec177973525b594b0ef343afc049ce70b0808e96a1b5e64' -- exitFeeReward
        )
),
compound AS (
    SELECT
        block_timestamp,
        tx_hash,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        topics,
        topic_0,
        DATA,
        CONCAT('0x', SUBSTR(topic_1, 27, 40)) AS user_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data [0] :: STRING)) * pow(
            10,
            -18
        ) AS rewardsInEth,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data [1] :: STRING)) * pow(
            10,
            -18
        ) AS stakeAmount,
        TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data [2] :: STRING)) * pow(
            10,
            -18
        ) AS exitFeeReward,
        _inserted_timestamp,
        _log_id
    FROM
        logs_pull
    WHERE
        topic_0 = '0x4139253da630b62761fa266dd3ef78ee3a1a13aa977ced7176f27084cba40265'
),
compound_split AS (
    -- extract eth only
    SELECT
        block_timestamp,
        tx_hash,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        topics,
        DATA,
        'claim-compound' AS action,
        user_address,
        rewardsInEth AS amount,
        'ETH' AS reward_token,
        _inserted_timestamp,
        _log_id
    FROM
        compound
    UNION ALL
        -- extract exitfee only in pear.
    SELECT
        block_timestamp,
        tx_hash,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        topics,
        DATA,
        'claim-compound' AS action,
        user_address,
        exitFeeReward AS amount,
        'PEAR' AS reward_token,
        _inserted_timestamp,
        _log_id
    FROM
        compound
),
FINAL AS (
    SELECT
        block_timestamp,
        tx_hash,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        action,
        user_address,
        amount,
        reward_token,
        _inserted_timestamp,
        _log_id
    FROM
        unstaked
    UNION ALL
    SELECT
        block_timestamp,
        tx_hash,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        action,
        user_address,
        amount,
        reward_token,
        _inserted_timestamp,
        _log_id
    FROM
        staked
    UNION ALL
    SELECT
        block_timestamp,
        tx_hash,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        action,
        user_address,
        amount,
        reward_token,
        _inserted_timestamp,
        _log_id
    FROM
        claims
    UNION ALL
    SELECT
        block_timestamp,
        tx_hash,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        action,
        user_address,
        amount,
        reward_token,
        _inserted_timestamp,
        _log_id
    FROM
        compound_split
)
SELECT
    block_timestamp,
    tx_hash,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    action,
    user_address,
    amount,
    reward_token,
    _inserted_timestamp,
    _log_id,
    {{ dbt_utils.generate_surrogate_key(
        ['_log_id']
    ) }} AS pear_staking_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
