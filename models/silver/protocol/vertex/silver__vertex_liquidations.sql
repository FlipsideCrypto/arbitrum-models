{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH logs_pull AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        'Liquidation' AS event_name,
        event_index,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        topics [1] :: STRING AS digest,
        LEFT(
            topics [2] :: STRING,
            42
        ) AS trader,
        topics [2] :: STRING AS subaccount,
        utils.udf_hex_to_int(
            topics [3] :: STRING
        ) :: INT AS MODE,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) :: INT AS health_group,
        utils.udf_hex_to_int(
            's2c',
            segmented_data [1] :: STRING
        ) :: INT AS amount,
        utils.udf_hex_to_int(
            's2c',
            segmented_data [2] :: STRING
        ) :: INT AS amount_quote,
        utils.udf_hex_to_int(
            segmented_data [3] :: STRING
        ) :: INT AS insurance_cover,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING = '0xf96988bb67b73fa61d64c48dc2e91ae6b697ebb8c5a496d238309aa20fbf6458'
        AND contract_address = '0xae1ec28d6225dce2ff787dcb8ce11cf6d3ae064f'

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
health_groups as (
    SELECT
        health_group,
        health_group_symbol
    FROM
        {{ ref('silver__vertex_dim_products') }}
    GROUP BY ALL    
),
FINAL AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        contract_address,
        event_name,
        event_index,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        digest,
        trader,
        subaccount,
        MODE,
        l.health_group,
        p.health_group_symbol,
        amount AS amount_unadj,
        amount / pow(
            10,
            18
        ) AS amount,
        amount_quote AS amount_quote_unadj,
        amount_quote / pow(
            10,
            18
        ) AS amount_quote,
        insurance_cover AS insurance_cover_unadj,
        insurance_cover / pow(
            10,
            18
        ) AS insurance_cover,
        _log_id,
        _inserted_timestamp
    FROM
        logs_pull l
    LEFT JOIN
        health_groups p 
    ON
        l.health_group = p.health_group
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash','event_index']
    ) }} AS vertex_liquidation_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1