{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['non_realtime','reorg']
) }}

WITH raw_logs AS (

    SELECT
        *
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        block_timestamp :: DATE >= '2021-11-01'
        AND contract_address IN (
            '0x09986b4e255b3c548041a30a2ee312fe176731c2',
            -- v2
            '0x2e3b85f85628301a0bce300dee3a6b04195a15ee' --v1
        )
        AND event_name IN ('ItemSold', 'BidAccepted')

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '24 hours'
    FROM
        {{ this }}
)
{% endif %}
),
base_sales AS (
    SELECT
        tx_hash,
        event_index,
        event_name,
        contract_address,
        decoded_flat,
        COALESCE(
            decoded_flat :bidType :: STRING,
            NULL
        ) AS auction_bid_type,
        COALESCE(
            decoded_flat :buyer :: STRING,
            decoded_flat :bidder :: STRING
        ) AS buyer_address,
        decoded_flat :seller :: STRING AS seller_address,
        decoded_flat :nftAddress :: STRING AS nft_address,
        decoded_flat :tokenId :: STRING AS tokenId,
        decoded_flat :quantity :: STRING AS quantity,
        decoded_flat :pricePerItem :: INT * quantity AS total_price_raw,
        COALESCE (
            decoded_flat :paymentToken :: STRING,
            '0x539bde0d7dbd336b79148aa742883198bbf60342'
        ) AS currency_address,
        -- for v2 there's a 3 month period where certain txs does not have a payment token field in the logs but it's confirmed to be all MAGIC tokens as payment. For v1 , all are magic tokens
        ROW_NUMBER() over (
            PARTITION BY tx_hash,
            currency_address
            ORDER BY
                event_index ASC
        ) AS intra_tx_grouping_raw,
        tx_hash || intra_tx_grouping_raw AS tx_hash_intra_tx_grouping_raw,
        block_number,
        block_timestamp,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        _log_id,
        _inserted_timestamp
    FROM
        raw_logs
),
eth_payment AS (
    SELECT
        tx_hash,
        from_address,
        to_address,
        eth_value,
        trace_index,
        IFF(
            to_address = '0xdb6ab450178babcf0e467c1f3b436050d907e233',
            'platform_tag',
            NULL
        ) AS platform_address_tag,
        IFF(
            to_address = '0xdb6ab450178babcf0e467c1f3b436050d907e233',
            eth_value,
            0
        ) AS platform_fee_raw_,
        CASE
            WHEN platform_address_tag IS NOT NULL THEN ROW_NUMBER() over (
                PARTITION BY tx_hash
                ORDER BY
                    trace_index ASC
            )
            ELSE NULL
        END AS intra_tx_grouping_raw
    FROM
        {{ ref('silver__traces') }}
    WHERE
        block_timestamp :: DATE >= '2021-11-01'
        AND from_address = '0x09986b4e255b3c548041a30a2ee312fe176731c2'
        AND eth_value > 0
        AND identifier != 'CALL_ORIGIN'
        AND TYPE != 'DELEGATECALL'
        AND trace_status = 'SUCCESS'
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                base_sales
            WHERE
                currency_address = '0x82af49447d8a07e3bd95bd0d56f35241523fbab1'
                AND event_name = 'ItemSold'
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '24 hours'
    FROM
        {{ this }}
)
{% endif %}
),
eth_payment_intra_fill AS (
    SELECT
        *,
        IFF(
            intra_tx_grouping_raw IS NULL,
            LAG(intra_tx_grouping_raw) ignore nulls over (
                PARTITION BY tx_hash
                ORDER BY
                    trace_index ASC
            ),
            intra_tx_grouping_raw
        ) AS intra_tx_grouping_fill
    FROM
        eth_payment
),
eth_payment_payment_fill AS (
    SELECT
        *,
        CASE
            WHEN ROW_NUMBER() over (
                PARTITION BY tx_hash,
                intra_tx_grouping_fill
                ORDER BY
                    eth_value DESC
            ) = 1
            AND platform_fee_raw_ = 0 THEN eth_value
            ELSE 0
        END AS sale_amount_raw_,
        CASE
            WHEN sale_amount_raw_ = 0
            AND platform_fee_raw_ = 0 THEN eth_value
            ELSE 0
        END AS creator_fee_raw_
    FROM
        eth_payment_intra_fill
),
eth_payment_payment_fill_agg AS (
    SELECT
        tx_hash,
        intra_tx_grouping_fill,
        SUM(sale_amount_raw_) AS sale_amount_eth,
        SUM(platform_fee_raw_) AS platform_fee_eth,
        SUM(creator_fee_raw_) AS creator_fee_eth
    FROM
        eth_payment_payment_fill
    WHERE
        intra_tx_grouping_fill IS NOT NULL
    GROUP BY
        ALL
),
zero_sale AS (
    SELECT
        tx_hash,
        intra_tx_grouping_fill
    FROM
        eth_payment_payment_fill_agg
    WHERE
        sale_amount_eth = 0
),
zero_sale_base_sales_buyers AS (
    SELECT
        tx_hash,
        seller_address
    FROM
        base_sales
    WHERE
        currency_address = '0x82af49447d8a07e3bd95bd0d56f35241523fbab1'
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                zero_sale
        )
),
zero_sale_raw AS (
    SELECT
        tx_hash,
        from_address,
        to_address,
        eth_value,
        trace_index,
        tx_hash || trace_index AS tx_hash_trace_index
    FROM
        eth_payment
    WHERE
        tx_hash IN (
            SELECT
                tx_hash
            FROM
                zero_sale
        )
),
zero_sale_seller_filter_raw AS (
    SELECT
        r.tx_hash,
        from_address,
        to_address,
        eth_value,
        trace_index
    FROM
        zero_sale_raw r
        LEFT JOIN zero_sale_base_sales_buyers b
        ON r.tx_hash = b.tx_hash
        AND r.to_address = b.seller_address
    WHERE
        b.seller_address IS NOT NULL
        AND to_address != '0xdb6ab450178babcf0e467c1f3b436050d907e233' qualify ROW_NUMBER() over (
            PARTITION BY r.tx_hash,
            trace_index
            ORDER BY
                trace_index ASC
        ) = 1
),
zero_sale_seller_filter AS (
    SELECT
        *,
        ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                trace_index ASC
        ) AS intra_tx_grouping_raw
    FROM
        zero_sale_seller_filter_raw
),
zero_sales_platform_only_row_num AS (
    SELECT
        *,
        ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                trace_index ASC
        ) AS intra_tx_grouping_raw
    FROM
        zero_sale_raw
    WHERE
        to_address = '0xdb6ab450178babcf0e467c1f3b436050d907e233'
),
zero_sales_seller_x_platform AS (
    SELECT
        tx_hash,
        intra_tx_grouping_raw,
        p.eth_value AS platform_fee_eth,
        s.eth_value AS sale_amount_eth
    FROM
        zero_sales_platform_only_row_num p
        INNER JOIN zero_sale_seller_filter s USING (
            tx_hash,
            intra_tx_grouping_raw
        )
),
zero_sales_base AS (
    -- base for untraditional eth sales
    SELECT
        tx_hash,
        intra_tx_grouping_raw,
        total_price_raw / pow(
            10,
            18
        ) AS total_price_eth,
        platform_fee_eth,
        sale_amount_eth,
        total_price_eth - sale_amount_eth - platform_fee_eth AS creator_fee_eth,
        total_price_raw,
        platform_fee_eth * pow(
            10,
            18
        ) AS platform_fee_raw,
        creator_fee_eth * pow(
            10,
            18
        ) AS creator_fee_raw
    FROM
        base_sales
        INNER JOIN zero_sales_seller_x_platform USING (
            tx_hash,
            intra_tx_grouping_raw
        )
    WHERE
        currency_address = '0x82af49447d8a07e3bd95bd0d56f35241523fbab1'
        AND event_name = 'ItemSold'
),
token_payment_raw AS (
    SELECT
        tx_hash,
        contract_address,
        from_address,
        to_address,
        raw_amount,
        event_index,
        IFF(
            to_address = '0xdb6ab450178babcf0e467c1f3b436050d907e233',
            'platform_tag',
            NULL
        ) AS platform_address_tag,
        IFF(
            to_address = '0xdb6ab450178babcf0e467c1f3b436050d907e233',
            raw_amount,
            0
        ) AS platform_fee_raw_,
        IFF(
            to_address = '0x539bde0d7dbd336b79148aa742883198bbf60342',
            'stop',
            NULL
        ) AS stop_tag,
        CASE
            WHEN platform_address_tag IS NOT NULL THEN ROW_NUMBER() over (
                PARTITION BY tx_hash
                ORDER BY
                    event_index ASC
            )
            ELSE NULL
        END AS intra_tx_grouping_raw
    FROM
        {{ ref('silver__transfers') }}
    WHERE
        block_timestamp :: DATE >= '2021-11-01'
        AND contract_address IN (
            '0x539bde0d7dbd336b79148aa742883198bbf60342',
            '0x82af49447d8a07e3bd95bd0d56f35241523fbab1'
        )
        AND tx_hash IN (
            SELECT
                tx_hash
            FROM
                base_sales
            WHERE
                currency_address = '0x539bde0d7dbd336b79148aa742883198bbf60342'
                OR (
                    currency_address = '0x82af49447d8a07e3bd95bd0d56f35241523fbab1'
                    AND event_name = 'BidAccepted'
                )
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '24 hours'
    FROM
        {{ this }}
)
{% endif %}
),
token_payment_intra_fill AS (
    SELECT
        *,
        IFF(
            intra_tx_grouping_raw IS NULL,
            LAG(intra_tx_grouping_raw) ignore nulls over (
                PARTITION BY tx_hash
                ORDER BY
                    event_index ASC
            ),
            intra_tx_grouping_raw
        ) AS intra_tx_grouping_fill
    FROM
        token_payment_raw
),
token_payment_payment_fill AS (
    SELECT
        *
    FROM
        token_payment_intra_fill
    WHERE
        intra_tx_grouping_fill IS NOT NULL
),
token_payment_intra_fill_platform_tag AS (
    SELECT
        tx_hash,
        intra_tx_grouping_fill,
        ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                event_index ASC
        ) AS platform_tag_row_number
    FROM
        token_payment_intra_fill
    WHERE
        platform_address_tag IS NOT NULL
),
base_sales_token AS (
    SELECT
        tx_hash,
        total_price_raw,
        intra_tx_grouping_raw AS platform_tag_row_number
    FROM
        base_sales
    WHERE
        currency_address = '0x539bde0d7dbd336b79148aa742883198bbf60342'
        OR (
            currency_address = '0x82af49447d8a07e3bd95bd0d56f35241523fbab1'
            AND event_name = 'BidAccepted'
        )
),
base_sales_token_total_price AS (
    SELECT
        *
    FROM
        token_payment_intra_fill_platform_tag
        JOIN base_sales_token USING (
            tx_hash,
            platform_tag_row_number
        )
),
token_payment_payment_fill_total_price AS (
    SELECT
        *,
        SUM(raw_amount) over (
            PARTITION BY tx_hash,
            intra_tx_grouping_fill
            ORDER BY
                event_index ASC
        ) AS cumulative_grouping_sale
    FROM
        token_payment_payment_fill
        LEFT JOIN base_sales_token_total_price USING (
            tx_hash,
            intra_tx_grouping_fill
        ) qualify cumulative_grouping_sale <= total_price_raw
),
token_payment_payment_fill_filtered AS (
    SELECT
        *,
        CASE
            WHEN ROW_NUMBER() over (
                PARTITION BY tx_hash,
                intra_tx_grouping_fill
                ORDER BY
                    raw_amount DESC
            ) = 1
            AND platform_fee_raw_ = 0 THEN raw_amount
            ELSE 0
        END AS sale_amount_raw_,
        CASE
            WHEN sale_amount_raw_ = 0
            AND platform_fee_raw_ = 0 THEN raw_amount
            ELSE 0
        END AS creator_fee_raw_
    FROM
        token_payment_payment_fill_total_price
),
token_payment_payment_fill_agg AS (
    SELECT
        tx_hash,
        intra_tx_grouping_fill AS intra_tx_grouping_raw,
        SUM(platform_fee_raw_) AS platform_fee_raw,
        SUM(creator_fee_raw_) AS creator_fee_raw
    FROM
        token_payment_payment_fill_filtered
    GROUP BY
        ALL
),
token_payment_payment_fill_grouping AS (
    SELECT
        tx_hash,
        ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                intra_tx_grouping_raw ASC
        ) AS intra_tx_grouping_raw,
        platform_fee_raw,
        creator_fee_raw
    FROM
        token_payment_payment_fill_agg
),
eth_payment_nonzero_filtered AS (
    SELECT
        tx_hash,
        ROW_NUMBER() over (
            PARTITION BY tx_hash
            ORDER BY
                intra_tx_grouping_fill ASC
        ) AS intra_tx_grouping_raw,
        platform_fee_eth * pow(
            10,
            18
        ) AS platform_fee_raw,
        creator_fee_eth * pow(
            10,
            18
        ) AS creator_fee_raw
    FROM
        eth_payment_payment_fill_agg
    WHERE
        tx_hash NOT IN (
            SELECT
                tx_hash
            FROM
                zero_sale
        )
),
eth_payment_combined AS (
    SELECT
        tx_hash,
        intra_tx_grouping_raw,
        platform_fee_raw,
        creator_fee_raw,
        'eth_zero_sales' AS tx_type,
        1 AS priority
    FROM
        zero_sales_base
    UNION ALL
    SELECT
        tx_hash,
        intra_tx_grouping_raw,
        platform_fee_raw,
        creator_fee_raw,
        'eth_regular' AS tx_type,
        2 AS priority
    FROM
        eth_payment_nonzero_filtered
),
base_sales_eth_payment AS (
    SELECT
        *
    FROM
        base_sales
        INNER JOIN eth_payment_combined USING (
            tx_hash,
            intra_tx_grouping_raw
        )
    WHERE
        currency_address = '0x82af49447d8a07e3bd95bd0d56f35241523fbab1'
        AND event_name = 'ItemSold' qualify ROW_NUMBER() over (
            PARTITION BY tx_hash,
            intra_tx_grouping_raw
            ORDER BY
                priority ASC
        ) = 1
),
base_sales_token_payment AS (
    SELECT
        *,
        'token_regular' AS tx_type
    FROM
        base_sales
        INNER JOIN token_payment_payment_fill_grouping USING (
            tx_hash,
            intra_tx_grouping_raw
        )
    WHERE
        currency_address = '0x539bde0d7dbd336b79148aa742883198bbf60342'
        OR (
            currency_address = '0x82af49447d8a07e3bd95bd0d56f35241523fbab1'
            AND event_name = 'BidAccepted'
        )
),
base_sales_combined AS (
    SELECT
        * exclude priority
    FROM
        base_sales_eth_payment
    UNION ALL
    SELECT
        *
    FROM
        base_sales_token_payment
),
nft_address_type AS (
    SELECT
        contract_address AS nft_address,
        token_transfer_type
    FROM
        {{ ref('silver__nft_transfers') }}
    WHERE
        block_timestamp :: DATE >= '2021-11-01'
        AND contract_address IN (
            SELECT
                nft_address
            FROM
                base_sales
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE
    FROM
        {{ this }}
)
{% endif %}

qualify ROW_NUMBER() over (
    PARTITION BY contract_address
    ORDER BY
        block_timestamp ASC
) = 1
),
tx_data AS (
    SELECT
        tx_hash,
        tx_fee,
        input_data
    FROM
        {{ ref('silver__transactions') }}
    WHERE
        block_timestamp :: DATE >= '2021-11-01'
        AND tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                base_sales_combined
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '24 hours'
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    intra_tx_grouping_raw,
    tx_type,
    contract_address AS platform_address,
    'treasure' AS platform_name,
    IFF(
        contract_address = '0x09986b4e255b3c548041a30a2ee312fe176731c2',
        'TreasureMarketplace v2',
        'TreasureMarketplace v1'
    ) AS platform_exchange_version,
    event_name,
    IFF(
        event_name = 'ItemSold',
        'sale',
        'bid_won'
    ) AS event_type,
    auction_bid_type,
    buyer_address,
    seller_address,
    nft_address,
    tokenId,
    quantity,
    token_transfer_type,
    CASE
        WHEN token_transfer_type = 'erc721_Transfer' THEN NULL
        ELSE quantity
    END AS erc1155_value,
    IFF(
        currency_address = '0x82af49447d8a07e3bd95bd0d56f35241523fbab1'
        AND event_name = 'ItemSold',
        'ETH',
        currency_address
    ) AS currency_address,
    total_price_raw,
    platform_fee_raw,
    creator_fee_raw,
    platform_fee_raw + creator_fee_raw AS total_fees_raw,
    tx_fee,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    CONCAT(
        nft_address,
        '-',
        tokenId,
        '-',
        platform_exchange_version,
        '-',
        _log_id
    ) AS nft_log_id,
    input_data,
    _log_id,
    _inserted_timestamp
FROM
    base_sales_combined
    INNER JOIN tx_data USING (tx_hash)
    LEFT JOIN nft_address_type USING (nft_address) qualify(ROW_NUMBER() over(PARTITION BY nft_log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
