{{ config(
    materialized = 'incremental',
    unique_key = '_call_id',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH nitro_blocks AS (

    SELECT
        block_id
    FROM
        {{ ref('bronze__blocks') }}
    WHERE
        block_timestamp >= '2022-08-30'
        AND block_id > '22207814'
        AND tx_count > 0

{% if is_incremental() %}
AND block_id NOT IN (
    SELECT
        DISTINCT block_number
    FROM
        {{ this }}
)
{% endif %}
ORDER BY
    _inserted_timestamp DESC
LIMIT
    500000
), nitro_block_txs AS (
    SELECT
        *
    FROM
        {{ ref('bronze__transactions') }}
    WHERE
        block_timestamp >= '2022-08-30'
        AND block_id > '22207814'
        AND block_id IN (
            SELECT
                block_id
            FROM
                nitro_blocks
        )

{% if is_incremental() %}
AND block_id NOT IN (
    SELECT
        DISTINCT block_number
    FROM
        {{ this }}
)
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY tx_id
ORDER BY
    _inserted_timestamp DESC)) = 1
),
key_groups AS (
    SELECT
        *,
        CASE
            WHEN t.tx :receipt :status :: STRING = '0x1' THEN 'SUCCESS'
            ELSE 'FAIL'
        END AS tx_status,
        CASE
            WHEN path NOT LIKE 'calls[%' THEN NULL
            WHEN NOT path LIKE ANY (
                '%from',
                '%gas%',
                '%input',
                '%output',
                '%to',
                '%calls',
                '%type',
                '%value'
            ) THEN path
            ELSE SUBSTR(
                path,
                0,
                (ARRAY_SIZE(SPLIT(path, '.')) - 1) * 8 + (ARRAY_SIZE(SPLIT(path, '.')) - 2)
            )
        END AS groupings,
        REPLACE (REPLACE(REPLACE(groupings, '[', '_'), ']', ''), '.calls_', '_') AS identifier_raw,
        REGEXP_REPLACE(
            identifier_raw,
            '[a-z]+',
            VALUE :type :: STRING
        ) AS identifier
    FROM
        nitro_block_txs t,
        TABLE (FLATTEN (input => t.tx :traces, recursive => TRUE)) b
    WHERE
        block_timestamp >= '2022-08-30'
        AND block_id > '22207814'
        AND block_id IN (
            SELECT
                block_id
            FROM
                nitro_blocks
        )
),
base_grouping_row AS (
    SELECT
        key,
        groupings,
        identifier_raw,
        identifier,
        path,
        INDEX,
        tx_id AS tx_hash,
        block_id AS block_number,
        block_timestamp,
        VALUE :from :: STRING AS from_address,
        VALUE :to :: STRING AS to_address,
        udf_hex_to_int(
            VALUE :gas :: STRING
        ) AS gas,
        udf_hex_to_int(
            VALUE :gasUsed :: STRING
        ) AS gas_used,
        VALUE :input :: STRING AS input,
        VALUE :output :: STRING AS output,
        VALUE :type :: STRING AS TYPE,
        CASE
            WHEN VALUE :type :: STRING = 'CALL' THEN silver.js_hex_to_int(
                VALUE :value :: STRING
            ) / 1e18
            ELSE 0
        END AS eth_value,
        tx_status,
        CASE
            WHEN VALUE :calls IS NOT NULL THEN ARRAY_SIZE(
                VALUE :calls
            )
            ELSE NULL
        END AS sub_traces,
        VALUE,
        ingested_at,
        _inserted_timestamp
    FROM
        key_groups
    WHERE
        key IS NULL
        AND INDEX IS NOT NULL
),
base_grouping_row_call_origin AS (
    SELECT
        key,
        'call_origin' AS groupings,
        'CALL_ORIGIN' AS identifier_raw,
        'CALL_ORIGIN' AS identifier,
        path,
        INDEX,
        tx_id AS tx_hash,
        block_id AS block_number,
        block_timestamp,
        this :from :: STRING AS from_address,
        this :to :: STRING AS to_address,
        udf_hex_to_int(
            this :gas
        ) AS gas,
        udf_hex_to_int(
            this :gasUsed
        ) AS gas_used,
        this :input :: STRING AS input,
        this :output :: STRING AS output,
        this :type :: STRING AS TYPE,
        CASE
            WHEN this :type :: STRING = 'CALL' THEN silver.js_hex_to_int(
                this :value :: STRING
            ) / 1e18
            ELSE 0
        END AS eth_value,
        tx_status,
        CASE
            WHEN this :calls IS NOT NULL THEN ARRAY_SIZE(
                this :calls
            )
            ELSE NULL
        END AS sub_traces,
        this,
        ingested_at,
        _inserted_timestamp
    FROM
        key_groups
    WHERE
        key = 'from'
        AND path = 'from'
),
base_grouping_row_final AS (
    SELECT
        *
    FROM
        base_grouping_row
    UNION ALL
    SELECT
        *
    FROM
        base_grouping_row_call_origin
),
arb_nitro_traces AS (
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        from_address,
        to_address,
        eth_value,
        gas,
        gas_used,
        input,
        output,
        TYPE,
        identifier,
        identifier_raw,
        CONCAT(
            tx_hash,
            '-',
            identifier
        ) AS _call_id,
        ingested_at,
        _inserted_timestamp,
        OBJECT_DELETE(
            VALUE,
            'calls'
        ) AS DATA,
        tx_status,
        sub_traces
    FROM
        base_grouping_row_final qualify (ROW_NUMBER() over (PARTITION BY _call_id
    ORDER BY
        _inserted_timestamp DESC)) = 1
)
SELECT
    tx_hash,
    block_number,
    block_timestamp,
    from_address,
    to_address,
    eth_value,
    gas,
    gas_used,
    input,
    output,
    TYPE,
    identifier,
    DATA,
    tx_status,
    sub_traces,
    _call_id,
    ingested_at,
    _inserted_timestamp
FROM
    arb_nitro_traces
