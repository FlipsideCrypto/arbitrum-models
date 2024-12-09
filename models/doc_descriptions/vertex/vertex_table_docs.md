{% docs vertex_dim_products %}

All available Vertex products, these are automatically added as they are released on chain.


{% enddocs %}

{% docs vertex_ez_liquidations %}

All Vertex liquidations. Once an account’s maintenance margin reaches $0, the account is eligible for liquidation. Liquidation events happen one by one, with the riskiest positions being liquidated first. Liquidations are based on the oracle price.


{% enddocs %}

{% docs vertex_ez_perp_trades %}

Vertex perpetuals are derivative contracts on an underlying spot asset. On Vertex, all perpetual contracts trade against USDC.

{% enddocs %}

{% docs vertex_ez_spot_trades %}

Vertex’s spot markets allow you to buy or sell listed crypto assets paired with USD-denominated stablecoins.

{% enddocs %}

{% docs vertex_ez_clearing_house_events %}

Vertex’s on-chain clearinghouse operates as the hub combining perpetual and spot markets, collateral, and risk calculations into a single integrated system. The events in this table track when a wallet either deposits or withdraws from the clearinghouse contract.

{% enddocs %}

{% docs vertex_ez_account_stats %}

Subaccount level table showing aggregated total activity across the Vertex exchange.

{% enddocs %}

{% docs vertex_ez_market_stats %}

Orderbook level market stats based on a combination of on-chain data and data from Vertex's ticker V2 API which includes 24-hour pricing and volume information on each market pair available on Vertex.

{% enddocs %}

{% docs vertex_ez_market_depth %}

Liquidity data taken from Vertex's Orderbook API, showing amount of liquidity at each price level.

{% enddocs %}

{% docs vertex_ez_staking  %}

All staking actions taken with the VRTX staking contract.

{% enddocs %}

{% docs vertex_ez_edge_trades  %}

All edge trades paired with the associated trader/subaccount.

{% enddocs %}

{% docs vertex_money_markets %}

Vertex integrates a decentralized money market directly into its DEX, enabling borrowing and lending of crypto assets using overcollateralized lending rules. Interest rates are dynamically adjusted based on supply and demand, incentivizing liquidity provision and balancing borrowing costs. The money market operates on-chain (e.g., on Arbitrum) and is managed through Vertex’s risk engine and clearinghouse, offering users automated borrowing via portfolio margin and passive yield opportunities on idle assets. This table tracks the money market products available on Vertex on an hourly basis.


{% enddocs %}
