{# {{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = '_log_id',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg'],
    enable=false
) }}

select
    ROW_NUMBER() OVER (ORDER BY product_id) AS row_num,
    product_id,
    ticker_id
from
    {{ ref('silver__vertex_din_') }}}}
where 
    product_id > 0
order by 
    product_id #}