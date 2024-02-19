{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = '_log_id',
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
        'ModifyCollateral' AS event_name,
        event_index,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        LEFT(
            topics [1] :: STRING,
            42
        ) AS trader,
        topics [1] :: STRING AS subaccount,
        utils.udf_hex_to_int(
            's2c',
            segmented_data [0] :: STRING
        ) :: INT AS amount,
        utils.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) :: INT AS product_id,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING = '0xfe53084a731040f869d38b1dcd00fbbdbc14e10d7d739160559d77f5bc80cf05'
        AND contract_address = '0xae1ec28d6225dce2ff787dcb8ce11cf6d3ae064f' --clearing house

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
product_id_join as (
    SELECT
        l.block_number,
        l.block_timestamp,
        l.tx_hash,
        l.contract_address,
        event_name,
        event_index,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        CASE
            WHEN amount < 0 THEN 'withdraw'
            WHEN amount > 0 THEN 'deposit'
        END AS modification_type,
        trader,
        subaccount,
        l.product_id,
        p.symbol,
        CASE 
            WHEN p.symbol = 'USDC' THEN '0xff970a61a04b1ca14834a43f5de4533ebddb5cc8'
            WHEN p.symbol = 'ETH' THEN '0x82af49447d8a07e3bd95bd0d56f35241523fbab1'
            WHEN p.symbol = 'BTC' THEN '0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f'
            WHEN p.symbol = 'ARB' THEN '0x912ce59144191c1204e64559fe8253a0e49e6548'
            WHEN p.symbol = 'USDT' THEN '0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9'
            WHEN p.symbol = 'VRTX' THEN '0x95146881b86b3ee99e63705ec87afe29fcc044d9'
        END AS token_address,
        amount,
        l._log_id,
        l._inserted_timestamp
    FROM
        logs_pull l
    LEFT JOIN 
        {{ ref('silver__vertex_dim_products') }} p 
    ON
        l.product_id = p.product_id
),
final as (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        a.contract_address,
        event_name,
        event_index,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        modification_type,
        trader,
        subaccount,
        product_id,
        a.symbol,
        a.token_address,
        amount as amount_unadj,
        amount / pow(
            10,
            18
        ) AS amount,
        (
        amount / pow(
            10,
            18
        ) * p.price
        ) :: FLOAT AS amount_usd,
        a._log_id,
        a._inserted_timestamp
    FROM
        product_id_join a
        LEFT JOIN {{ ref('price__ez_hourly_token_prices') }}
        p
        ON a.token_address = p.token_address
        AND DATE_TRUNC(
            'hour',
            block_timestamp
        ) = p.hour
        LEFT JOIN 
            {{ ref('silver__contracts') }} c
        ON
            a.token_address = c.contract_address
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
    final qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1