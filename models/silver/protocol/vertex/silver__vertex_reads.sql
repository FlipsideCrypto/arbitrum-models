WITH function_sigs AS (
    SELECT
        '0x02ee3a52' AS function_sig,
        'getAllProducts' AS function_name
),
inputs AS (
    SELECT
        lower('0x1693273B443699bee277eCbc60e2C8027E91995d') AS contract_address,
        function_sig,
        function_name,
        0 AS function_input,
        CONCAT(
            function_sig,
            LPAD(
                function_input,
                64,
                0
            )
        ) AS DATA
        FROM function_sigs
)
,
contract_reads AS (
    SELECT
        contract_address,
        (select max(block_number) from arbitrum_dev.silver.blocks) as block_number,
        function_sig,
        function_name,
        function_input,
        DATA,
        utils.udf_json_rpc_call(
            'eth_call',
            [{ 'to': contract_address, 'from': null, 'data': data }, utils.udf_int_to_hex(block_number) ]
        ) AS rpc_request,
        live.udf_api(
            node_url,
            rpc_request
        ) AS read_output,
        SYSDATE() AS _inserted_timestamp
    FROM
        inputs
        JOIN streamline.crosschain.node_mapping
        ON 1 = 1
        AND chain = 'arbitrum'
),
read_result as (
    SELECT
        read_output,
        read_output :data :id :: STRING AS read_id,
        read_output :data :result :: STRING AS read_result,
        SPLIT(
            read_id,
            '-'
        ) AS read_id_object,
        regexp_substr_all(SUBSTR(read_result, 3, len(read_result)), '.{64}') AS segmented_data,
        function_sig,
        function_name,
        function_input,
        DATA,
        contract_address,
        block_number,
        _inserted_timestamp
    FROM
        contract_reads
),
lat_flat as (
SELECT 
    s.value AS segmented_data_expand,
    function_sig,
    function_name,
    function_input,
    DATA,
    contract_address,
    block_number,
    _inserted_timestamp
FROM READ_RESULT,
LATERAL FLATTEN(segmented_data) AS s
)
select
    CONCAT('0x', SUBSTR(segmented_data_expand :: STRING, 25, 40)) AS underlying_product_addr,
    c.*,
    l.*
from
    lat_flat l 
INNER JOIN 
    ARBITRUM_DEV.SILVER.CONTRACTS C
ON
    c.contract_address = CONCAT('0x', SUBSTR(segmented_data_expand :: STRING, 25, 40))