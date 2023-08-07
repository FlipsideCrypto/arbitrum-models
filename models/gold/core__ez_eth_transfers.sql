{{ config(
    materialized = 'view'
) }}

WITH eth_base AS (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        from_address,
        to_address,
        eth_value,
        identifier,
        input,
        REPLACE(
            COALESCE(
                DATA :value :: STRING,
                DATA :action :value :: STRING
            ),
            '0x'
        ) AS hex,
        to_varchar(TO_NUMBER(hex, REPEAT('X', LENGTH(hex)))) AS eth_value_precise_raw,
        IFF(LENGTH(eth_value_precise_raw) > 18, LEFT(eth_value_precise_raw, LENGTH(eth_value_precise_raw) - 18) || '.' || RIGHT(eth_value_precise_raw, 18), '0.' || LPAD(eth_value_precise_raw, 18, '0')) AS rough_conversion,
        IFF(
            POSITION(
                '.000000000000000000' IN rough_conversion
            ) > 0,
            LEFT(rough_conversion, LENGTH(rough_conversion) - 19),
            REGEXP_REPLACE(
                rough_conversion,
                '0*$',
                ''
            )
        ) AS eth_value_precise
    FROM
        {{ ref('core__fact_traces') }}
    WHERE
        TYPE = 'CALL'
        AND eth_value > 0
        AND tx_status = 'SUCCESS'
        AND trace_status = 'SUCCESS'
),
eth_price AS (
    SELECT
        HOUR,
        price AS eth_price
    FROM
        {{ ref('silver__prices_eth') }}
)
SELECT
    A.tx_hash AS tx_hash,
    A.block_number AS block_number,
    A.block_timestamp AS block_timestamp,
    A.identifier AS identifier,
    tx.from_address AS origin_from_address,
    tx.to_address AS origin_to_address,
    tx.origin_function_signature AS origin_function_signature,
    A.from_address AS eth_from_address,
    A.to_address AS eth_to_address,
    A.eth_value AS amount,
    A.eth_value_precise_raw AS amount_precise_raw,
    A.eth_value_precise AS amount_precise,
    ROUND(
        A.eth_value * eth_price,
        2
    ) AS amount_usd
FROM
    eth_base A
    LEFT JOIN eth_price
    ON DATE_TRUNC(
        'hour',
        block_timestamp
    ) = HOUR
    JOIN {{ ref('silver__transactions') }}
    tx
    ON A.tx_hash = tx.tx_hash
