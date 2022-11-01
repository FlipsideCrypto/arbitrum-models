{{ config(
    materialized = 'incremental',
    unique_key = '_call_id',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH new_blocks AS (

    SELECT
        block_id
    FROM
        {{ ref('bronze__blocks') }}
    WHERE
        block_id <= '22207814'
    AND   
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
),

non_nitro_final as (
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
),



nitro_blocks as (
select 
    block_id
    from {{ ref('bronze__blocks') }}
    where block_timestamp >= '2022-08-30'
    and block_id > '22207814'
    and tx_count > 0

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
),

nitro_block_txs AS (
    SELECT
        *
    FROM
        {{ ref('bronze__transactions') }}
    WHERE
        block_timestamp >= '2022-08-30'
    
    AND 
        block_id > '22207814'
    AND 
        block_id IN (
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

 key_groups as (
    select *,
    case 
        when t.tx:receipt:status ::string = '0x1' then 'SUCCESS'
        else 'FAIL'
        end as tx_status,
    case
        when path not like 'calls[%' then null 
        when not path like any 
        ('%from', '%gas%', '%input', '%output', '%to', '%calls', '%type', '%value') then path
        else 
        substr( path, 0,
       (array_size(split(path, '.')) - 1) * 8 + (array_size(split(path, '.')) - 2)
      ) 
        end as groupings,
    replace (replace(replace(groupings, '[', '_') ,']', ''), '.calls_','_') as identifier_raw,
    regexp_replace(identifier_raw, '[a-z]+', value:type::string) as identifier
    
    from nitro_block_txs t, 
        table (flatten (input => t.tx:traces, recursive => TRUE)) b 
    
    where block_timestamp >= '2022-08-30'
    and block_id > '22207814'
    and block_id in (select block_id from nitro_blocks)

    order by block_id, block_timestamp, identifier_raw asc
    ),
    
    base_grouping_row as (
    select 
    key,
    groupings,
    identifier_raw,
    identifier,
    path, 
    index,
    tx_id as tx_hash,
    block_id as block_number, 
    block_timestamp, 
    
    value:from::string as from_address, 
    value:to::string as to_address, 
    udf_hex_to_int(value:gas::string) as gas,
    udf_hex_to_int(value:gasUsed::string) as gas_used,
    value:input::string as input, 
    value:output::string as output,
    value:type::string as type,
    case 
        when value:type :: string = 'CALL' 
            then silver.js_hex_to_int(value:value ::string) / 1e18
        else 0 
        end as eth_value,
    tx_status,
    case 
        when value:calls is not null then array_size(value:calls)
        else null 
        end as sub_traces,
    value, 
    ingested_at, 
    _inserted_timestamp
    
    
    from key_groups
    where key is null 
    and index is not null 
    order by block_id, block_timestamp, tx_hash, identifier_raw asc
    ),
    
    
    base_grouping_row_call_origin as (
    select 
    key,
    'call_origin' as groupings,
    'CALL_ORIGIN' as identifier_raw,
    'CALL_ORIGIN' as identifier,
    path, 
    index,
    tx_id as tx_hash,
    block_id as block_number, 
    block_timestamp, 
    this:from::string as from_address, 
    this:to::string as to_address, 
    udf_hex_to_int(this:gas) as gas,
    udf_hex_to_int(this:gasUsed) as gas_used,
    this:input::string as input, 
    this:output::string as output,
    this:type::string as type,
    case 
        when this:type :: string = 'CALL' 
            then silver.js_hex_to_int(this:value ::string) / 1e18
        else 0 
        end as eth_value,
    tx_status,
    case 
        when this:calls is not null then array_size(this:calls)
        else null 
        end as sub_traces,
    this, 
    ingested_at, 
    _inserted_timestamp
    from key_groups
    where key = 'from'
    and path = 'from'
        ),
    
    base_grouping_row_final as (

    select * 
        from base_grouping_row 
    union all 
    select * 
        from base_grouping_row_call_origin
        
        order by block_number, block_timestamp, tx_hash, identifier_raw asc
        ),
       
    arb_nitro_traces as (
    select 
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
    type, 
    identifier,
    identifier_raw, 
    concat(tx_hash,'-', identifier) as _call_id,
    ingested_at, 
    _inserted_timestamp, 
    object_delete(value, 'calls') as data,
    tx_status, 
    sub_traces 
    from base_grouping_row_final 
    
    qualify (ROW_NUMBER() over (PARTITION BY _call_id
        ORDER BY
            _inserted_timestamp DESC)) = 1
    

    )

    select 
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
    type, 
    identifier,
    data, 
    tx_status, 
    sub_traces,
    _call_id,
    ingested_at, 
    _inserted_timestamp 
    from non_nitro_final

    union all

    select 
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
    type, 
    identifier,
    data, 
    tx_status, 
    sub_traces,
    _call_id,
    ingested_at, 
    _inserted_timestamp 
    from arb_nitro_traces
    
