{% docs defi__fact_trades %}

This table reports trading activity that occurs in both the decentralized exchange and automated money markets. Trades fulfill one of four operations: manage buy offers, manage sell offers and path payments (strict send and receive). Trades can be executed either against the orderbook or a liquidity pool that contains both bid and ask asset pair. Trades can be either path payments or offers that are fully or partially fulfilled, which means that the trade can be split into segments. A full trade is compromised of all "order" numbers for a given history_operation_id.

Learn more about Stellar data concepts: https://developers.stellar.org/docs/learn/fundamentals/stellar-data-structures

{% enddocs %}

