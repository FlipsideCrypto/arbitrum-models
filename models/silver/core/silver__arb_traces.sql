-- depends_on: {{ ref('bronze__streamline_traces') }}
{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['partition_key'],
    full_refresh = false
) }}

WITH bronze_traces AS (

    SELECT
        block_number,
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
    _partition_by_block_id <= 30000000
    AND block_number <= 22207817
    AND DATA :result IS NOT NULL
    AND DATA :result <> '[]'
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY block_number, VALUE :array_index :: INT, DATA :transactionPosition :: INT
ORDER BY
    _inserted_timestamp DESC)) = 1
)
SELECT
    block_number,
    DATA: transactionPosition :: INT AS tx_position,
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
    bronze_traces qualify(ROW_NUMBER() over(PARTITION BY traces_id
ORDER BY
    _inserted_timestamp DESC)) = 1
