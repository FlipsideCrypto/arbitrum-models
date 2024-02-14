{{ config(
    materialized = 'table',
    unique_key = 'block_number',
    tags = ['curated','reorg']
) }}

WITH final as (

    SELECT
        product_id,
        contract_address,
        symbol,
        type,
        price_increment_x18,
        size_increment,
        min_size,
        min_depth_x18,
        max_spread_rate_x18,
        maker_fee_rate_x18,
        taker_fee_rate_x18,
        long_weight_initial_x18,
        long_weight_maintenance_x18
    FROM
        {{ ref('silver__vertex_product_contract_id_seed') }}

)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['product_id','contract_address']
    ) }} AS vertex_product_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL
