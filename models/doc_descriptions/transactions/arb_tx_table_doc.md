{% docs arb_tx_table_doc %}

This table contains transaction level data for the Arbitrum Blockchain. Each transaction will have a unique transaction hash, along with transactions fees and a ETH value transferred when applicable. Transactions may be native ETH transfers or interactions with contract addresses. For more information, please see [The Ethereum Organization - Transactions](https://ethereum.org/en/developers/docs/transactions/).

**Note:** If you are comparing transaction count against block explorers such as Arbiscan, you will need to exclude system transactions using `where origin_function_signature <> '0x6bf6a42d'` in your query.

{% enddocs %}