{% docs vertex_symbol %}

The specific Vertex product symbol, if it is a futures product it will have a -PERP suffix.

{% enddocs %}

{% docs vertex_digest %}

The identifier for a specific trade, this can be split across two or more base deltas in order to fill the entire amount of the trade.

{% enddocs %}

{% docs vertex_trader %}

The wallet address of the trader, there can be multiple subaccounts associated with a trader.

{% enddocs %}

{% docs vertex_subaccount %}

Independent Vertex account of trader with its own margin, balance, positions, and trades. Any wallet can open an arbitrary number of these. Risk is not carried over from subaccount to subaccount.

{% enddocs %}

{% docs vertex_trade_type %}

They type of trade taken, long/short for perps or buy/sell for spot.

{% enddocs %}

{% docs vertex_expiration %}

Time after which the order should automatically be cancelled, as a timestamp in seconds after the unix epoch, converted to datetime.

{% enddocs %}

{% docs vertex_nonce %}

Number used to differentiate between the same order multiple times, and a user trying to place an order with the same parameters twice. Represented as a string.

{% enddocs %}

{% docs vertex_istaker %}

Boolean representing if the trader was the taker or maker.

{% enddocs %}

{% docs vertex_price_amount_unadj %}

The price amount that the trade was executed at.

{% enddocs %}

{% docs vertex_price_amount %}

The price amount that the trade was executed at, decimal adjusted. All amounts and prices are adjusted 18 decimals points regardless of underlying asset contract. 

{% enddocs %}

{% docs vertex_amount_unadj %}

The total size of the trade in units of the asset being traded.

{% enddocs %}

{% docs vertex_amount %}

The total size of the trade in units of the asset being traded across one digest, decimal adjusted. All amounts and prices are adjusted 18 decimals points regardless of underlying asset contract. 

{% enddocs %}

{% docs vertex_amount_usd %}

The size of the trade in USD. Base Delta multiplied by the price amount. 

{% enddocs %}

{% docs vertex_fee_amount_unadj %}

The fees on the trade.

{% enddocs %}

{% docs vertex_fee_amount %}

The fees on the trade, decimal adjusted. All amounts and prices are adjusted 18 decimals points regardless of underlying asset contract. 

{% enddocs %}

{% docs vertex_base_delta_amount_unadj %}

Represents the net change in the total quantity of orders at a particular price level, the sum of these across the same digest is equal to the amount. This is the first currency listed in the pair and acts as the reference point for the exchange rate, in this case the crypto asset trading against USDC.

{% enddocs %}

{% docs vertex_base_delta_amount %}

Represents the net change in the total quantity of orders at a particular price level, decimal adjusted. All amounts and prices are adjusted 18 decimals points regardless of underlying asset contract. The sum of these across the same digest is equal to the amount. This is the first currency listed in the pair and acts as the reference point for the exchange rate, in this case the crypto asset trading against USDC.

{% enddocs %}

{% docs vertex_quote_delta_amount_unadj %}

A positive value is an increase in spread and a negative value is a decrease in spread. Quote is currency used to express the value of the base currency. It's often the more well-known or stable currency in the pair. In this case, USDC.

{% enddocs %}

{% docs vertex_quote_delta_amount %}

The net change in the best bid and best ask prices in the order book, decimal adjusted. All amounts and prices are adjusted 18 decimals points regardless of underlying asset contract. A positive value is an increase in spread and a negative value is a decrease in spread. Quote is currency used to express the value of the base currency. It's often the more well-known or stable currency in the pair. In this case, USDC.

{% enddocs %}

{% docs vertex_mode %}

The type of liquidation, 0 being a LP position, 1 being a balance - ie a Borrow, and 2 being a perp position.

{% enddocs %}

{% docs vertex_health_group %}

The spot / perp product pair of health group i where health_groups[i][0] is the spot product_id and health_groups[i][1] is the perp product_id. Additionally, it is possible for a health group to only have either a spot or perp product, in which case, the product that doesn’t exist is set to 0.

{% enddocs %}

{% docs vertex_amount_quote_unadj %}

To liquidate a position, there must be a payment (transfer) between the liquidator and the position holder. This done in the quote currency, USDC. Payments are signed as positive, meaning you received the USDC, or negative, meaning you paid. For perpetual liquidations, users should expect to see a (+) USDC payment. They will see a (-) USDC payment for borrowers since they need to pay the user for buying their borrow.

{% enddocs %}

{% docs vertex_amount_quote %}

To liquidate a position, there must be a payment (transfer) between the liquidator and the position holder. This done in the quote currency, USDC. Payments are signed as positive, meaning you received the USDC, or negative, meaning you paid. For perpetual liquidations, users should expect to see a (+) USDC payment. They will see a (-) USDC payment for borrowers since they need to pay the user for buying their borrow. All amounts and prices are adjusted 18 decimals points regardless of underlying asset contract.

{% enddocs %}

{% docs vertex_insurance_cover_unadj %}

USDC from the insurance fund pulled into the insolvent account and used to pay liquidators to take on the underwater positions.

{% enddocs %}

{% docs vertex_insurance_cover %}

USDC from the insurance fund pulled into the insolvent account and used to pay liquidators to take on the underwater positions, decimal adjusted. All amounts and prices are adjusted 18 decimals points regardless of underlying asset contract.

{% enddocs %}

{% docs vertex_book_address %}

The contract address associated with each product, this is where all fill orders are published to the chain.

{% enddocs %}

{% docs vertex_product_type %}

The type of product, either spot or perpetual futures.

{% enddocs %}

{% docs vertex_product_id %}

The unique id of each product. Evens are perp products and odds are spot products.

{% enddocs %}

{% docs vertex_ticker_id %}

Identifier of a ticker with delimiter to separate base/target.

{% enddocs %}

{% docs vertex_name %}

The name of the product

{% enddocs %}

{% docs vertex_version %}

The product version.

{% enddocs %}

{% docs vertex_token_address %}

The underlying asset token address deposited or withdrawn from the clearinghouse contract.

{% enddocs %}