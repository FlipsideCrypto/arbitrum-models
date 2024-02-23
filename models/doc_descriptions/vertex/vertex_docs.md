{% docs vertex_symbol %}

The specific Vertex product symbol, if it is a futures product it will have a -PERP suffix.

{% enddocs %}

{% docs vertex_digest %}

The identifier for a specific trade, this can be split across two or more base deltas in order to fill the entire amount fo the trade.

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

Time after which the order should automatically be cancelled, as a timestamp in seconds after the unix epoch, sent as a string.

{% enddocs %}

{% docs vertex_nonce %}

Number used to differentiate between the same order multiple times, and a user trying to place an order with the same parameters twice. Sent as a a string.

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

The net change in the total quantity of orders at a particular price level, the sum of these across the same digest is equal to the amount.

{% enddocs %}

{% docs vertex_base_delta_amount %}

The net change in the total quantity of orders at a particular price level, decimal adjusted. All amounts and prices are adjusted 18 decimals points regardless of underlying asset contract. The sum of these across the same digest is equal to the amount.

{% enddocs %}

{% docs vertex_base_quote_delta_amount_unadj %}

A positive value is an increase in spread and a negative value is a decrease in spread.

{% enddocs %}

{% docs vertex_base_quote_delta_amount %}

The net change in the best bid and best ask prices in the order book,, decimal adjusted. All amounts and prices are adjusted 18 decimals points regardless of underlying asset contract. A positive value is an increase in spread and a negative value is a decrease in spread.

{% enddocs %}