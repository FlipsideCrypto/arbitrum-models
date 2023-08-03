{% docs eth_dex_lp_tokens %}

The address for the token included in the liquidity pool, as a JSON object. 

Query example to access the key:value pairing within the object:
SELECT
    DISTINCT pool_address AS unique_pools,
    tokens :token0 :: STRING AS token0,
    symbols: token0 :: STRING AS token0_symbol,
    decimals: token0 :: STRING AS token0_decimal
FROM arbitrum.defi.dim_dex_liquidity_pools
WHERE token0 = '0x82af49447d8a07e3bd95bd0d56f35241523fbab1'
;

{% enddocs %}