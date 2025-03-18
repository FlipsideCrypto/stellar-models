{% docs core__fact_accounts %}

The accounts table stores detailed information for a given account, including current account status, preconditions for transaction authorization, security settings and account balance. The balance reported in the accounts table reflects the account’s XLM balance only. All other asset balances are reported in the 'trust_lines' table. Any changes to the account, whether it is an account settings change, balance increase/decrease or sponsorship change will result in an increase to the account sequence_number and last_modified_ledger. The sequence_number is incremented with each operation applied to an account so that order is preserved during account mutation.

Learn more about Stellar accounts: https://developers.stellar.org/docs/learn/fundamentals/stellar-data-structures/accounts

{% enddocs %}
