-- depends_on: {{ ref('bronze__streamline_traces') }}
{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['partition_key']
) }}

WITH bronze_traces AS (

    SELECT
        block_number,
        DATA :transactionPosition :: INT as tx_position,
        DATA,
        VALUE,
        _partition_by_block_id AS partition_key,
        id,
        _inserted_timestamp
    FROM

{% if is_incremental() %}

{{ ref('bronze__streamline_traces') }}

WHERE
    1 = 2
{% else %}
    {{ ref('bronze__streamline_fr_traces') }}
WHERE
    _partition_by_block_id <= 22210000
    AND block_number <= 22207817
    AND IFNULL(IS_OBJECT(DATA :action), FALSE)
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY block_number, tx_position, value:array_index::int
ORDER BY
    _inserted_timestamp DESC)) = 1
)
SELECT
    block_number,
    tx_position,
    IFF(
        DATA :traceAddress = '[]',
        'ORIGIN',
        REPLACE(
            REPLACE(REPLACE(DATA :traceAddress :: STRING, '['), ']'),
            ',',
            '_'
        )
    ) AS trace_address,
    CASE
        WHEN trace_address = 'ORIGIN' THEN NULL
        WHEN POSITION(
            '_' IN trace_address
        ) = 0 THEN 'ORIGIN'
        ELSE REGEXP_REPLACE(
            trace_address,
            '_[0-9]+$',
            '',
            1,
            1
        )
    END AS parent_trace_address,
    DATA :traceAddress AS trace_address_array,
    DATA AS trace_json,
    partition_key,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['block_number', 'tx_position', 'trace_address']
    ) }} AS traces_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    bronze_traces