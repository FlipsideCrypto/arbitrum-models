{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "create_block",
    cluster_by = ['create_timestamp::DATE'],
    tags = ['reorg','curated']
) }}
-- Pulls contract details for relevant c assets.  The case when handles cETH.
WITH log_pull AS (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        contract_address,
        _inserted_timestamp,
        _log_id
    FROM
        {{ ref('silver__logs') }}
        l
    WHERE
        topics [0] = '0x7ac369dbd14fa5ea3f473ed67cc9d598964a77501540ba6751eb0b3decf5870d'
        AND origin_from_address = '0x0f01756bc6183994d90773c8f22e3f44355ffa0e'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
),
traces_pull AS (
    SELECT
        from_address AS token_address,
        to_address AS underlying_asset
    FROM
        {{ ref('silver__traces') }}
    WHERE
        tx_hash IN (
            SELECT
                tx_hash
            FROM
                log_pull
        )
        AND identifier = 'STATICCALL_0_2'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
),
contracts AS (
    SELECT
        *
    FROM
        {{ ref('silver__contracts') }}
),
contract_pull AS (
    SELECT
        l.tx_hash,
        l.block_number,
        l.block_timestamp,
        l.contract_address,
        C.token_name,
        C.token_symbol,
        C.token_decimals,
        CASE
            WHEN l.contract_address = '0x2193c45244af12c280941281c8aa67dd08be0a64' THEN LOWER('0x82aF49447D8a07e3bd95BD0d56f35241523fBab1') --WETH
            ELSE t.underlying_asset
        END AS underlying_asset,
        l._inserted_timestamp,
        l._log_id
    FROM
        log_pull l
        LEFT JOIN traces_pull t
        ON l.contract_address = t.token_address
        LEFT JOIN contracts C
        ON C.contract_address = l.contract_address qualify(ROW_NUMBER() over(PARTITION BY l.contract_address
    ORDER BY
        block_timestamp ASC)) = 1
)
SELECT
    l.block_number AS create_block,
    l.block_timestamp as create_timestamp,
    l.tx_hash AS create_hash,
    l.token_name AS itoken_name,
    l.token_symbol AS itoken_symbol,
    l.token_decimals AS itoken_decimals,
    l.contract_address AS itoken_address,
    C.token_name AS underlying_name,
    C.token_symbol AS underlying_symbol,
    C.token_decimals AS underlying_decimals,
    l.underlying_asset AS underlying_asset_address,
    l._inserted_timestamp,
    l._log_id
FROM
    contract_pull l
    LEFT JOIN contracts C
    ON C.contract_address = l.underlying_asset
WHERE
    l.token_name IS NOT NULL
