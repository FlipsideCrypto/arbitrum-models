
{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  persist_docs ={ "relation": true,
  "columns": true },
  unique_key = '_log_id',
  cluster_by = ['block_timestamp::DATE']
) }}

WITH borrow_txns AS (

  SELECT
    DISTINCT tx_hash,
    contract_address
  FROM
    {{ ref('silver__logs') }}
  WHERE
    topics [0] :: STRING = '0x3a5151e57d3bc9798e7853034ac52293d1a0e12a2b44725e75b03b21f86477a6'

{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= (
  SELECT
    MAX(_inserted_timestamp) :: DATE - 2
  FROM
    {{ this }}
)
{% endif %}
),
repay_txns AS (
  SELECT
    DISTINCT tx_hash,
    contract_address
  FROM
    {{ ref('silver__logs') }}
  WHERE
    topics [0] :: STRING = '0xc8e512d8f188ca059984b5853d2bf653da902696b8512785b182b2c813789a6e'

{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= (
  SELECT
    MAX(_inserted_timestamp) :: DATE - 2
  FROM
    {{ this }}
)
{% endif %}
),
add_asset AS (
  SELECT
    DISTINCT tx_hash,
    contract_address
  FROM
    {{ ref('silver__logs') }}
  WHERE
    topics [0] :: STRING = '0x30a8c4f9ab5af7e1309ca87c32377d1a83366c5990472dbf9d262450eae14e38'

{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= (
  SELECT
    MAX(_inserted_timestamp) :: DATE - 2
  FROM
    {{ this }}
)
{% endif %}
),
remove_asset AS (
  SELECT
    DISTINCT tx_hash,
    contract_address
  FROM
    {{ ref('silver__logs') }}
  WHERE
    topics [0] :: STRING = '0x6e853a5fd6b51d773691f542ebac8513c9992a51380d4c342031056a64114228'

{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= (
  SELECT
    MAX(_inserted_timestamp) :: DATE - 2
  FROM
    {{ this }}
)
{% endif %}
),
borrow AS (
  SELECT
    block_timestamp,
    block_number,
    tx_hash,
    'Borrow' AS action,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    CONCAT ('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS asset,
    CONCAT ('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS lending_pool_address,
    origin_from_address AS borrower,
    CONCAT ('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS borrower2,
    TRY_TO_NUMBER(
      PUBLIC.udf_hex_to_int(SUBSTR(DATA, 3, len(DATA))) :: INTEGER) AS amount,
      CASE
        WHEN borrower = borrower2 THEN 'no'
        ELSE 'yes'
      END AS borrower_is_a_contract,
      _log_id,
      _inserted_timestamp
      FROM
        {{ ref('silver__logs') }}
      WHERE
        topics [0] :: STRING = '0x6eabe333476233fd382224f233210cb808a7bc4c4de64f9d76628bf63c677b1a'
        AND tx_hash IN (
          SELECT
            tx_hash
          FROM
            borrow_txns
        )
        AND CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) IN (
          SELECT
            pair_address
          FROM
            {{ ref('sushi__dim_kashi_pairs') }}
        )

{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= (
  SELECT
    MAX(_inserted_timestamp) :: DATE - 2
  FROM
    {{ this }}
)
{% endif %}
),
add_coll_same_txn AS (
  SELECT
    block_timestamp,
    block_number,
    tx_hash,
    'add collateral' AS action,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    CONCAT ('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS asset,
    CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS lending_pool_address,
    origin_from_address AS borrower,
    CONCAT ('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS borrower2,
    TRY_TO_NUMBER(
      PUBLIC.udf_hex_to_int(SUBSTR(DATA, 3, len(DATA))) :: INTEGER) AS amount,
      CASE
        WHEN borrower = borrower2 THEN 'no'
        ELSE 'yes'
      END AS borrower_is_a_contract,
      _log_id,
      _inserted_timestamp
      FROM
        {{ ref('silver__logs') }}
      WHERE
        topics [0] :: STRING = '0x6eabe333476233fd382224f233210cb808a7bc4c4de64f9d76628bf63c677b1a'
        AND tx_hash IN (
          SELECT
            tx_hash
          FROM
            borrow_txns
        )
        AND CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) IN (
          SELECT
            pair_address
          FROM
            {{ ref('sushi__dim_kashi_pairs') }}
        )

{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= (
  SELECT
    MAX(_inserted_timestamp) :: DATE - 2
  FROM
    {{ this }}
)
{% endif %}
),
repay AS (
  SELECT
    block_timestamp,
    block_number,
    tx_hash,
    'Repay' AS action,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    CONCAT ('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS asset,
    CONCAT ('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS lending_pool_address,
    origin_from_address AS borrower,
    CONCAT ('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS borrower2,
    TRY_TO_NUMBER(
      PUBLIC.udf_hex_to_int(SUBSTR(DATA, 3, len(DATA))) :: INTEGER) AS amount,
      CASE
        WHEN borrower = borrower2 THEN 'no'
        ELSE 'yes'
      END AS lender_is_a_contract,
      _log_id,
      _inserted_timestamp
      FROM
        {{ ref('silver__logs') }}
      WHERE
        topics [0] :: STRING = '0x6eabe333476233fd382224f233210cb808a7bc4c4de64f9d76628bf63c677b1a'
        AND tx_hash IN (
          SELECT
            tx_hash
          FROM
            repay_txns
        )
        AND CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) IN (
          SELECT
            pair_address
          FROM
            {{ ref('sushi__dim_kashi_pairs') }}
        )

{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= (
  SELECT
    MAX(_inserted_timestamp) :: DATE - 2
  FROM
    {{ this }}
)
{% endif %}
),
remove_coll_same_txn AS (
  SELECT
    block_timestamp,
    block_number,
    tx_hash,
    'Remove collateral' AS action,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS asset,
    CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS lending_pool_address,
    origin_from_address AS borrower,
    CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS borrower2,
    TRY_TO_NUMBER(
      PUBLIC.udf_hex_to_int(SUBSTR(DATA, 3, len(DATA))) :: INTEGER) AS amount,
      CASE
        WHEN borrower = borrower2 THEN 'no'
        ELSE 'yes'
      END AS borrower_is_a_contract,
      _log_id,
      _inserted_timestamp
      FROM
        {{ ref('silver__logs') }}
      WHERE
        topics [0] :: STRING = '0x6eabe333476233fd382224f233210cb808a7bc4c4de64f9d76628bf63c677b1a'
        AND tx_hash IN (
          SELECT
            tx_hash
          FROM
            repay_txns
        )
        AND CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) IN (
          SELECT
            pair_address
          FROM
            {{ ref('sushi__dim_kashi_pairs') }}
        )

{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= (
  SELECT
    MAX(_inserted_timestamp) :: DATE - 2
  FROM
    {{ this }}
)
{% endif %}
),
add_coll_in_separate_txn AS (
  SELECT
    block_timestamp,
    block_number,
    tx_hash,
    'add collateral' AS action,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS asset,
    CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS lending_pool_address,
    origin_from_address AS borrower,
    CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS borrower2,
    TRY_TO_NUMBER(
      PUBLIC.udf_hex_to_int(SUBSTR(DATA, 3, len(DATA))) :: INTEGER) AS amount,
      CASE
        WHEN borrower = borrower2 THEN 'no'
        ELSE 'yes'
      END AS borrower_is_a_contract,
      _log_id,
      _inserted_timestamp
      FROM
        {{ ref('silver__logs') }}
      WHERE
        topics [0] :: STRING = '0x6eabe333476233fd382224f233210cb808a7bc4c4de64f9d76628bf63c677b1a'
        AND tx_hash NOT IN (
          SELECT
            tx_hash
          FROM
            borrow_txns
        )
        AND tx_hash NOT IN (
          SELECT
            tx_hash
          FROM
            repay_txns
        )
        AND tx_hash NOT IN (
          SELECT
            tx_hash
          FROM
            add_asset
        )
        AND CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) IN (
          SELECT
            pair_address
          FROM
            {{ ref('sushi__dim_kashi_pairs') }}
        )

{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= (
  SELECT
    MAX(_inserted_timestamp) :: DATE - 2
  FROM
    {{ this }}
)
{% endif %}
),
remove_coll_in_separate_txn AS (
  SELECT
    block_timestamp,
    block_number,
    tx_hash,
    'Remove collateral' AS action,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS asset,
    CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS lending_pool_address,
    origin_from_address AS borrower,
    CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS borrower2,
    TRY_TO_NUMBER(
      PUBLIC.udf_hex_to_int(SUBSTR(DATA, 3, len(DATA))) :: INTEGER) AS amount,
      CASE
        WHEN borrower = borrower2 THEN 'no'
        ELSE 'yes'
      END AS borrower_is_a_contract,
      _log_id,
      _inserted_timestamp
      FROM
        {{ ref('silver__logs') }}
      WHERE
        topics [0] :: STRING = '0x6eabe333476233fd382224f233210cb808a7bc4c4de64f9d76628bf63c677b1a'
        AND tx_hash NOT IN (
          SELECT
            tx_hash
          FROM
            borrow_txns
        )
        AND tx_hash NOT IN (
          SELECT
            tx_hash
          FROM
            repay_txns
        )
        AND tx_hash NOT IN (
          SELECT
            tx_hash
          FROM
            remove_asset
        )
        AND CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) IN (
          SELECT
            pair_address
          FROM
            {{ ref('sushi__dim_kashi_pairs') }}
        )

{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= (
  SELECT
    MAX(_inserted_timestamp) :: DATE - 2
  FROM
    {{ this }}
)
{% endif %}
),
total AS (
  SELECT
    *
  FROM
    borrow
  UNION ALL
  SELECT
    *
  FROM
    add_coll_same_txn
  UNION ALL
  SELECT
    *
  FROM
    remove_coll_same_txn
  UNION ALL
  SELECT
    *
  FROM
    repay
  UNION ALL
  SELECT
    *
  FROM
    add_coll_in_separate_txn
  UNION ALL
  SELECT
    *
  FROM
    remove_coll_in_separate_txn
),

 prices AS (
    select 
        symbol,
        date_trunc('hour',recorded_at) as hour, 
        avg(price) as price 
    from 
        {{ source('prices','prices_v2') }} a 
    join {{ ref('sushi__dim_kashi_pairs') }} b
    on a.symbol = b.asset_symbol
    WHERE
        1 = 1

{% if is_incremental() %}
AND hour :: DATE IN (
    SELECT
        DISTINCT block_timestamp :: DATE
    FROM
        borrow
)
{% else %}
    AND hour :: DATE >= '2021-09-01'
{% endif %}
    group by 1,2
),


labels as (
select *
from {{ ref('sushi__dim_kashi_pairs') }}
),


Labled_WO_prices as (
select 
a.block_timestamp,
a.block_number,
a.tx_hash,
a.action,
a.origin_from_address,
a.origin_to_address,
a.origin_function_signature,
a.Borrower2 as Borrower,
a.Borrower_is_a_contract,
a.lending_pool_address,
b.pair_name as lending_pool,
a.asset,
CASE
  when action in ('add collateral','Remove collateral') then b.collateral_symbol
  else b.asset_symbol 
  end AS symbol,
CASE
when b.collateral_decimals is null THEN a.amount
when b.asset_decimals is null then a.amount
WHEN b.collateral_decimals is not null and action = 'add collateral' THEN (A.amount/ pow(10, b.collateral_decimals))
WHEN b.collateral_decimals is not null and action = 'Remove collateral' THEN (A.amount/ pow(10, b.collateral_decimals))
WHEN b.asset_decimals is not null and action = 'Borrow' then (A.amount/ pow(10, b.asset_decimals))
WHEN b.asset_decimals is not null and action = 'Repay' then (A.amount/ pow(10, b.asset_decimals))
END AS amount,
a._log_id,
_inserted_timestamp
from total a
left join labels b 
on a.Lending_pool_address = b.pair_address
)



select 
a.block_timestamp,
a.block_number,
a.tx_hash,
a.action,
a.origin_from_address,
a.origin_to_address,
a.origin_function_signature,
a.Borrower,
a.Borrower_is_a_contract,
a.lending_pool_address,
a.lending_pool,
a.asset,
a.symbol,
a.amount,
  CASE
    WHEN action = 'add collateral' THEN (
      A.amount * C.price 
    )
    WHEN action = 'Remove collateral' THEN (
      A.amount * C.price 
      )
    ELSE (A.amount * C.price)
  END AS amount_USD,
a._log_id,
_inserted_timestamp
from Labled_WO_prices a
LEFT JOIN prices c
ON a.symbol = c.symbol
AND DATE_TRUNC(
    'hour',
    a.block_timestamp
) = c.hour
