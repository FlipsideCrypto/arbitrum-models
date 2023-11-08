{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['non_realtime','reorg']
) }}

WITH supply AS (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        l.contract_address AS compound_market,
        CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS asset,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) :: INTEGER AS supply_amount,
        origin_from_address AS depositor_address,
        'Compound V3' AS compound_version,
        C.contract_address AS underlying_asset_address,
        C.token_name,
        C.token_symbol,
        C.token_decimals,
        'ethereum' AS blockchain,
        _log_id,
        l._inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
        l
        LEFT JOIN {{ ref('silver__contracts') }} C
        ON asset = address
    WHERE
        topics [0] = '0xfa56f7b24f17183d81894d3ac2ee654e3c26388d17a28dbd9549b8114304e1f4' --SupplyCollateral

{% if is_incremental() %}
AND l._inserted_timestamp >= (
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
    compound_market,
    depositor_address,
    w.underlying_asset_address AS deposit_asset,
    supply_amount / pow(
        10,
        w.token_decimals
    ) AS supplied_tokens,
    compound_version,
    w.token_symbol,
    blockchain,
    _log_id,
    _inserted_timestamp
FROM
    supply w
