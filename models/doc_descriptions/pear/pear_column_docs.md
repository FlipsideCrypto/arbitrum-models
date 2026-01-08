{% docs pear_trader %}

The wallet address of the trader executing trades through the Pear protocol.

{% enddocs %}

{% docs pear_first_trade_timestamp %}

The block timestamp of this trader's first trade.

{% enddocs %}

{% docs pear_trade_type %}

The type of trade taken, buy/long or sell/short for perpetual futures.

{% enddocs %}

{% docs pear_platform %}

The underlying protocol where the trade was executed (gmx-v2, symmio, or vertex).

{% enddocs %}

{% docs pear_symbol %}

The trading pair symbol for the perpetual futures contract.

{% enddocs %}

{% docs pear_market_type %}

The type of market action - market_increase for opening positions or market_decrease for closing positions.

{% enddocs %}

{% docs pear_is_taker %}

Boolean representing if the trader was the taker or maker in the trade.

{% enddocs %}

{% docs pear_price_amount_unadj %}

The raw price amount that the trade was executed at before decimal adjustment.

{% enddocs %}

{% docs pear_price_amount %}

The price amount that the trade was executed at, decimal adjusted based on the underlying protocol's decimals.

{% enddocs %}

{% docs pear_amount_unadj %}

The raw size of the trade in units of the asset being traded before decimal adjustment.

{% enddocs %}

{% docs pear_amount %}

The total size of the trade in units of the asset being traded, decimal adjusted based on the underlying protocol's decimals.

{% enddocs %}

{% docs pear_amount_usd %}

The USD value of the trade, calculated as amount * price.

{% enddocs %}

{% docs pear_fee_amount_unadj %}

The raw trading fees charged on the trade before decimal adjustment.

{% enddocs %}

{% docs pear_fee_amount %}

The trading fees charged on the trade, decimal adjusted based on the underlying protocol's decimals.

{% enddocs %}

{% docs pear_day %}

The date for which APR and fee statistics are calculated.

{% enddocs %}

{% docs pear_daily_fee %}

The total fees collected by the protocol for the day, adjusted for decimals.

{% enddocs %}

{% docs pear_stake_action %}

The type of staking action performed (staked, unstaked, claim-withdraw, claim-compound).

{% enddocs %}

{% docs pear_current_rebate_rate %}

The current trading fee rebate rate based on monthly trading volume tiers.

{% enddocs %}

{% docs pear_current_fee_discount %}

The current trading fee discount rate based on PEAR token staking tiers.

{% enddocs %}

{% docs pear_staked_amount %}

The amount of PEAR tokens currently staked by the user.

{% enddocs %}

{% docs pear_apr %}

The current annual percentage rate for staking PEAR tokens, calculated from daily protocol fees.

{% enddocs %}

{% docs pear_liquidation_amount %}

The size of the liquidation in the asset's base units.

{% enddocs %}

{% docs pear_liquidation_amount_usd %}

The USD value of the liquidation at the time it occurred.

{% enddocs %} 