-- depends_on: {{ ref('bronze__traces') }}
{{ config (
    materialized = "incremental",
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['modified_timestamp::DATE','partition_key'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
    full_refresh = false,
    tags = ['core','non_realtime']
) }}

WITH bronze_traces AS (

    SELECT
        block_number,
        partition_key,
        VALUE :array_index :: INT AS tx_position,
        DATA :result AS full_traces,
        _inserted_timestamp
    FROM

{% if is_incremental() and not full_reload_mode %}
{{ ref('bronze__traces') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )
    AND DATA :result IS NOT NULL
    AND block_number > 22207817 {% elif is_incremental() and full_reload_mode %}
    {{ ref('bronze__traces_fr') }}
WHERE
    {% if use_partition_key %}
        partition_key BETWEEN (
            SELECT
                MAX(partition_key) - 100000
            FROM
                {{ this }}
        )
        AND (
            SELECT
                MAX(partition_key) + 10000000
            FROM
                {{ this }}
        )
    {% else %}
        partition_key BETWEEN (
            SELECT
                MAX(partition_key) - 100000
            FROM
                {{ this }}
        )
        AND (
            SELECT
                MAX(partition_key) + 10000000
            FROM
                {{ this }}
        )
    {% endif %}
    AND block_number > 22207817
{% else %}
    {{ ref('bronze__traces_fr') }}
WHERE
    partition_key <= 30000000
    AND block_number > 22207817
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY block_number, tx_position
ORDER BY
    _inserted_timestamp DESC)) = 1
),
flatten_traces AS (
    SELECT
        block_number,
        tx_position,
        partition_key,
        IFF(
            path IN (
                'result',
                'result.value',
                'result.type',
                'result.to',
                'result.input',
                'result.gasUsed',
                'result.gas',
                'result.from',
                'result.output',
                'result.error',
                'result.revertReason',
                'result.time',
                'gasUsed',
                'gas',
                'type',
                'to',
                'from',
                'value',
                'input',
                'error',
                'output',
                'time',
                'revertReason',
                'afterEVMTransfers',
                'beforeEVMTransfers',
                'result.afterEVMTransfers',
                'result.beforeEVMTransfers'
            ),
            'ORIGIN',
            REGEXP_REPLACE(REGEXP_REPLACE(path, '[^0-9]+', '_'), '^_|_$', '')
        ) AS trace_address,
        _inserted_timestamp,
        OBJECT_AGG(
            key,
            VALUE
        ) AS trace_json,
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
        SPLIT(
            trace_address,
            '_'
        ) AS trace_address_array
    FROM
        bronze_traces txs,
        TABLE(
            FLATTEN(
                input => PARSE_JSON(
                    txs.full_traces
                ),
                recursive => TRUE
            )
        ) f
    WHERE
        f.index IS NULL
        AND f.key != 'calls'
        AND f.path != 'result'
        AND f.path NOT LIKE 'afterEVMTransfers[%'
        AND f.path NOT LIKE 'beforeEVMTransfers[%'
    GROUP BY
        block_number,
        tx_position,
        partition_key,
        trace_address,
        _inserted_timestamp
)
SELECT
    block_number,
    tx_position,
    trace_address,
    parent_trace_address,
    trace_address_array,
    trace_json,
    partition_key,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['block_number', 'tx_position', 'trace_address']
    ) }} AS traces_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    flatten_traces qualify(ROW_NUMBER() over(PARTITION BY traces_id
ORDER BY
    _inserted_timestamp DESC)) = 1
