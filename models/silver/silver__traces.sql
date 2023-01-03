{{ config(
    materialized = 'incremental',
    unique_key = 'tx_hash',
    cluster_by = ['block_timestamp::DATE'],
    incremental_strategy = 'delete+insert'
) }}

WITH new_blocks AS (

    SELECT
        block_id
    FROM
        {{ ref('bronze__blocks') }}
    WHERE
        tx_count > 0

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
), traces_txs AS (
    SELECT
        *
    FROM
        {{ ref('bronze__transactions') }}
    WHERE
        block_id IN (
            SELECT
                block_id
            FROM
                new_blocks
        ) qualify(ROW_NUMBER() over(PARTITION BY tx_id
    ORDER BY
        _inserted_timestamp DESC)) = 1
),
traces_raw AS (
    SELECT
        block_id,
        block_timestamp,
        tx_id AS tx_hash,
        tx: traces AS full_traces,
        ingested_at :: TIMESTAMP AS ingested_at,
        _inserted_timestamp :: TIMESTAMP AS _inserted_timestamp,
        CASE
            WHEN tx :receipt :status :: STRING = '0x1' THEN 'SUCCESS'
            ELSE 'FAIL'
        END AS tx_status
    FROM
        traces_txs
),
traces_flat AS (
    SELECT
        VALUE :from :: STRING AS from_address,
        udf_hex_to_int(
            VALUE :gas :: STRING
        ) AS gas,
        udf_hex_to_int(
            VALUE :gasUsed :: STRING
        ) AS gas_used,
        VALUE :input :: STRING AS input,
        VALUE :output :: STRING AS output,
        VALUE :time :: STRING AS TIME,
        VALUE :to :: STRING AS to_address,
        VALUE :type :: STRING AS TYPE,
        VALUE :traceAddress AS traceAddress,
        VALUE: subtraces :: INTEGER AS sub_traces,
        CASE
            WHEN VALUE :type :: STRING IN (
                'call',
                'delegatecall',
                'staticcall'
            ) THEN udf_hex_to_int(
                VALUE :value :: STRING
            ) / pow(
                10,
                18
            )
            ELSE 0
        END AS eth_value,*
    FROM
        traces_raw,
        LATERAL FLATTEN (
            input => full_traces
        )
)
SELECT
    tx_hash,
    block_id AS block_number,
    block_timestamp,
    from_address,
    to_address,
    eth_value,
    gas,
    gas_used,
    input,
    output,
    UPPER(TYPE) AS TYPE,
    sub_traces,
    REPLACE(REPLACE(REPLACE(traceAddress :: STRING, ']'), '['), ',', '_') AS id,
    CASE
        WHEN INDEX = 0 THEN 'CALL_ORIGIN'
        ELSE concat_ws('_', UPPER(TYPE), id)END AS identifier,
        concat_ws(
            '-',
            tx_hash,
            identifier
        ) AS _call_id,
        ingested_at,
        VALUE AS DATA,
        tx_status,
        _inserted_timestamp
        FROM
            traces_flat
        WHERE
            identifier IS NOT NULL qualify (ROW_NUMBER() over (PARTITION BY _call_id
        ORDER BY
            _inserted_timestamp DESC)) = 1