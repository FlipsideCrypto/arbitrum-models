{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH queued_deposits AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        'Queued' AS event_name,
        event_index,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        --unique hash of the order
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) :: INT AS spot,
        utils.udf_hex_to_int(
            's2c',
            segmented_data [1] :: STRING
        ) :: INT AS amount,
        utils.udf_hex_to_int(
            segmented_data [2] :: STRING
        ) AS expiration,
        utils.udf_hex_to_int(
            segmented_data [3] :: STRING
        ) :: INT AS nonce,
        utils.udf_hex_to_int(
            's2c',
            segmented_data [4] :: STRING
        ) :: INT AS isTaker,
        utils.udf_hex_to_int(
            's2c',
            segmented_data [5] :: STRING
        ) :: INT AS feeAmount,
        utils.udf_hex_to_int(
            's2c',
            segmented_data [6] :: STRING
        ) :: INT AS baseDelta,
        utils.udf_hex_to_int(
            's2c',
            segmented_data [7] :: STRING
        ) :: INT AS quoteDelta,
        l._log_id,
        l._inserted_timestamp
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] = '0x5a595dfbcf52edc2be4703ae288841f3b01e1c4a8bf9a45b09914abd29b8d009'
    AND
        contract_address = '0x052ab3fd33cadf9d9f227254252da3f996431f75' --elixir vertex manager

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
token_join AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        d.contract_address,
        event_name,
        event_index,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        caller,
        reciever,
        token AS token_address,
        token_symbol,
        amount AS amount_unadj,
        amount / pow(
            10,
            C.token_decimals
        ) AS amount,
        (
            amount / pow(
                10,
                C.token_decimals
            ) * p.price
        ) :: FLOAT AS amount_usd,
        d._log_id,
        d._inserted_timestamp
    FROM
        deposits d
        LEFT JOIN {{ ref('price__ez_hourly_token_prices') }}
        p
        ON d.token = p.token_address
        AND DATE_TRUNC(
            'hour',
            block_timestamp
        ) = p.hour
        LEFT JOIN {{ ref('silver__contracts') }} C
        ON d.token = C.contract_address
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash','event_index']
    ) }} AS vertex_deposit_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    token_join qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
