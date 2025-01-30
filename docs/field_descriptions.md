{# Common Fields #}

{% docs pk %}
Unique primary key for the dimension table, used as the main identifier for each record.
{% enddocs %}

{% docs inserted_timestamp %}
Timestamp when this record was inserted.
{% enddocs %}

{% docs modified_timestamp %}
Timestamp when this record was last modified.
{% enddocs %}

{% docs closed_at %}
Timestamp when the ledger was closed and committed to the network. Ledgers are expected to close ~every 5 seconds.
{% enddocs %}

{% docs ledger_sequence %}
The sequence number of the ledger.
{% enddocs %}

{% docs batch_id %}
String representation of the run id for a given DAG in Airflow. Takes the form of "scheduled__<batch_end_date>-<dag_alias>". Batch ids are unique to the batch and help with monitoring and rerun capabilities.
{% enddocs %}

{% docs batch_run_date %}
The start date for the batch interval. When taken with the date in the batch_id, the date represents the interval of ledgers processed. The batch run date can be seen as a proxy of closed_at for a ledger.
{% enddocs %}

{% docs batch_insert_ts %}
The timestamp in UTC when a batch of records was inserted into the database. This field can help identify if a batch executed in real time or as part of a backfill. The timestamp should not be used during ad hoc analysis and is useful for data engineering purposes.
{% enddocs %}

{% docs invocation_id %}
Unique identifier of the dbt job that inserted or modified this record.
{% enddocs %}

{% docs asset_type %}
The identifier for type of asset code, can be an alphanumeric with 4 characters, 12 characters or the native asset to the network, XLM.
{% enddocs %}

{% docs asset_code %}
The 4 or 12 character code representation of the asset on the network.
{% enddocs %}

{% docs asset_issuer %}
The account address of the original asset issuer that created the asset.
{% enddocs %}

{% docs asset_id %}
The identifier of the asset involved in the operation.
{% enddocs %}

{% docs deleted %}
Indicates whether the ledger entry (account, claimable balance, trust line, offer, liquidity pool) has been deleted or not. Once an entry is deleted, it cannot be recovered.
{% enddocs %}

{% docs last_modified_ledger %}
The ledger sequence number when the ledger entry was last modified. Deletions do not count as a modification and will report the prior modification sequence number.
{% enddocs %}

{% docs ledger_entry_change %}
Code that describes the ledger entry change type that was applied to the ledger entry.
{% enddocs %}

{# Account-specific fields #}

{% docs account_id %}
The address of the account. The address is the account's public key encoded in base32. All account addresses start with a 'G'.
{% enddocs %}

{% docs balance %}
The number of units of XLM held by the account.
{% enddocs %}

{% docs buying_liabilities %}
The sum of all buy offers owned by this account for XLM only.
{% enddocs %}

{% docs selling_liabilities %}
The sum of all sell offers owned by this account for XLM only.
{% enddocs %}

{% docs sequence_number %}
The account's current sequence number. The sequence number controls operations applied to an account. Operations must submit a unique sequence number that is incremented by 1 in order to apply the operation to the account so that account changes will not collide within a ledger.
{% enddocs %}

{% docs num_subentries %}
The total number of ledger entries connected to this account. Ledger entries include: trustlines, offers, signers, and data entries. (Claimable balances are counted under sponsoring entries, not subentries). Any newly created trustline, offer, signer or data entry will increase the number of subentries by 1. Accounts may have up to 1,000 subentries.
{% enddocs %}

{% docs inflation_destination %}
Deprecated: The account address to receive an inflation payment when they are disbursed on the network.
{% enddocs %}

{% docs flags %}
Denotes the enabling and disabling of certain asset issuer privileges.
{% enddocs %}

{% docs home_domain %}
The domain that hosts this account's stellar.toml file.
{% enddocs %}

{% docs master_weight %}
The weight of the master key, which is the private key for this account. If a master key is '0', the account is locked and cannot be used.
{% enddocs %}

{% docs threshold_low %}
The sum of the weight of all signatures that sign a transaction for the low threshold operation. The weight must exceed the set threshold for the operation to succeed.
{% enddocs %}

{% docs threshold_medium %}
The sum of the weight of all signatures that sign a transaction for the medium threshold operation. The weight must exceed the set threshold for the operation to succeed.
{% enddocs %}

{% docs threshold_high %}
The sum of the weight of all signatures that sign a transaction for the high threshold operation. The weight must exceed the set threshold for the operation to succeed.
{% enddocs %}

{% docs sponsor %}
The account address of the sponsor who is paying the reserves for this ledger entry.
{% enddocs %}

{% docs num_sponsored %}
The number of reserves sponsored for this account (meaning another account is paying for the minimum balance). Sponsored entries do not incur any reserve requirement on the account that owns the entry.
{% enddocs %}

{% docs num_sponsoring %}
The number of reserves sponsored by this account. Entries sponsored by this account incur a reserve requirement.
{% enddocs %}

{% docs sequence_ledger %}
The unsigned 32-bit ledger number of the sequence number's age.
{% enddocs %}

{% docs sequence_time %}
The UNIX timestamp of the sequence number's age.
{% enddocs %}

{% docs core_accounts_id %}
Unique identifier for this record
{% enddocs %}

{# Ledger-specific fields #}

{% docs ledger_hash %}
The hex-encoded SHA-256 hash that represents the ledger's XDR-encoded form.
{% enddocs %}

{% docs previous_ledger_hash %}
The hex-encoded SHA-256 hash of the ledger that immediately precedes this ledger.
{% enddocs %}

{% docs transaction_count %}
The number of successful transactions submitted and completed by the network in this ledger.
{% enddocs %}

{% docs operation_count %}
The total number of successful operations applied to this ledger.
{% enddocs %}

{% docs base_fee %}
The fee (in stroops) the network charges per operation in a transaction for the given ledger. The minimum base fee is 100, with the ability to increase if transaction demand exceeds ledger capacity. When this occurs, the ledger enters surge pricing.
{% enddocs %}

{% docs base_reserve %}
The reserve (in stroops) the network requires an account to retain as a minimum balance in order to be a valid account on the network. The current minimum reserve is 10 XLM.
{% enddocs %}

{% docs sequence %}
The sequence number that corresponds to the individual ledgers. As ledgers are written to the network, the sequence is incremented by 1
{% enddocs %}

{% docs id %}
Unique identifier for the ledger
{% enddocs %}

{% docs total_coins %}
Total number of lumens in circulation
{% enddocs %}

{% docs fee_pool %}
The sum of all transaction fees
{% enddocs %}

{% docs max_tx_set_size %}
The maximum number of operations that Stellar validator nodes have agreed to process in a given ledger. Since Protocol 11, ledger capacity has been measured in operations rather than transactions.
{% enddocs %}

{% docs protocol_version %}
The protocol verstion that the Stellar network was running when this ledger was committed. Protocol versions are released ~every 6 months.
{% enddocs %}

{% docs ledger_header %}
A base64-encoded string of the raw LedgerHeader xdr struct for this ledger
{% enddocs %}

{% docs successful_transaction_count %}
The number of successful transactions submitted and completed by the network in this ledger
{% enddocs %}

{% docs failed_transaction_count %}
The number of failed transactions submitted to the network in this ledger. The transaction was still paid for but contained an error that prevented it from executing
{% enddocs %}

{% docs tx_set_operation_count %}
The total number of operations in the transaction set for this ledger, including failed transactions.
{% enddocs %}

{% docs soroban_fee_write_1kb %}
The fee associated with writing 1KB of data to the Soroban ledger.The write fee is dynamic fee that grows linearly with the size of the ledger. The write fee for a given ledger is recorded in the ledger header and has a direct relationship to the bucketlist size of the ledger db.
{% enddocs %}

{% docs node_id %}
The id of winning validator node which is allowed to write transaction set to the network. The winning validator is decided by the network.
{% enddocs %}

{% docs signature %}
The signing hash of the validator node which writes the transaction set to the network.This signature ensures the integrity and authenticity of the ledger, confirming that it has not been tampered with.
{% enddocs %}

{% docs total_byte_size_of_bucket_list %}
Total size in bytes of the bucket list
{% enddocs %}

{% docs fact_ledgers_id %}
{{ doc("pk") }}
{% enddocs %}

{# Liquidity Pool-specific fields #}

{% docs liquidity_pool_id %}
Unique identifier for a liquidity pool. There cannot be duplicate pools for the same asset pair. Once a pool has been created for the asset pair, another cannot be created.
{% enddocs %}

{% docs pool_share_count %}
Participation in a liquidity pool is represented by a pool share.
{% enddocs %}

{% docs trustline_count %}
Total number of accounts with trustlines authorized to the pool. To create a trustline, an account must trust both base assets before trusting a pool with the asset pair.
{% enddocs %}

{% docs type %}
The mechanism that calculates pricing and division of shares for the pool. With the initial AMM rollout, the only type of liquidity pool allowed to be created is a constant product pool.
{% enddocs %}

{% docs fee %}
The number of basis points charged as a percentage of the trade in order to complete the transaction. The fees earned on all trades are divided amongst pool shareholders and distributed as an incentive to keep money in the pools.
{% enddocs %}

{% docs asset_a_amount %}
The raw number of tokens locked in the pool for one of the two asset pairs in the liquidity pool.
{% enddocs %}

{% docs asset_b_amount %}
The raw number of tokens locked in the pool for one of the two asset pairs in the liquidity pool.
{% enddocs %}

{% docs partition_id %}
Partition identifier for the record.
{% enddocs %}

{% docs liquidity_pools_id %}
Unique identifier for the liquidity pool record.
{% enddocs %}

{# Operation-specific fields #}

{% docs op_id %}
Unique identifier for the operation.
{% enddocs %}

{% docs transaction_hash %}
Hash of the transaction.
{% enddocs %}

{% docs successful %}
Whether the operation was successful.
{% enddocs %}

{% docs fee_charged %}
Actual fee charged for the operation.
{% enddocs %}

{% docs operation_result_code %}
Result code of the operation.
{% enddocs %}

{% docs operation_trace_code %}
Trace code of the operation.
{% enddocs %}

{% docs account %}
The account involved in the operation.
{% enddocs %}

{% docs amount %}
Float representation of the amount of an asset sent/offered/etc.
{% enddocs %}

{% docs authorize %}
Indicates whether the trustline is authorized.
{% enddocs %}

{% docs balance_id %}
The balance id of the claimable balance.
{% enddocs %}

{% docs buying_asset_code %}
The 4 or 12 character code representation of the asset being bought.
{% enddocs %}

{% docs buying_asset_issuer %}
The account address of the original asset issuer for the buying asset.
{% enddocs %}

{% docs buying_asset_type %}
The identifier for type of buying asset code.
{% enddocs %}

{% docs buying_asset_id %}
The identifier of the asset being bought.
{% enddocs %}

{% docs from %}
The account address from which the payment/contract originates.
{% enddocs %}

{% docs funder %}
The account address that funds a new account.
{% enddocs %}

{% docs into %}
The account address receiving the deleted account's lumens.
{% enddocs %}

{% docs limit %}
The upper bound amount of an asset that an account can hold.
{% enddocs %}

{% docs name %}
The key for a data entry in manage data operations.
{% enddocs %}

{% docs offer_id %}
The unique id for the offer.
{% enddocs %}

{% docs path %}
Path for path payment operations.
{% enddocs %}

{% docs price %}
The ratio of selling asset to buying asset.
{% enddocs %}

{% docs d %}
Denominator in price ratio calculations.
{% enddocs %}

{% docs n %}
Numerator in price ratio calculations.
{% enddocs %}

{% docs selling_asset_code %}
The 4 or 12 character code representation of the asset being sold.
{% enddocs %}

{% docs selling_asset_issuer %}
The account address of the original asset issuer for the selling asset.
{% enddocs %}

{% docs selling_asset_type %}
The identifier for type of selling asset code.
{% enddocs %}

{% docs selling_asset_id %}
The identifier of the asset being sold.
{% enddocs %}

{% docs set_flags %}
Array of numeric values of flags set for a trustline.
{% enddocs %}

{% docs set_flags_s %}
Array of string values of flags set for a trustline.
{% enddocs %}

{% docs signer_key %}
The address of the signer.
{% enddocs %}

{% docs signer_weight %}
The weight of the new signer.
{% enddocs %}

{% docs source_amount %}
The originating amount sent designated in the source asset.
{% enddocs %}

{% docs source_asset_code %}
The 4 or 12 character code representation of the source asset.
{% enddocs %}

{% docs source_asset_issuer %}
The account address of the original source asset issuer.
{% enddocs %}

{% docs source_asset_type %}
The identifier for type of source asset code.
{% enddocs %}

{% docs source_asset_id %}
The identifier of the source asset.
{% enddocs %}

{% docs source_max %}
The maximum amount to be sent in source asset.
{% enddocs %}

{% docs starting_balance %}
The amount of XLM sent to newly created account.
{% enddocs %}

{% docs to %}
The address of the account receiving payment funds.
{% enddocs %}

{% docs trustee %}
The issuing account address.
{% enddocs %}

{% docs trustor %}
The trusting account address.
{% enddocs %}

{% docs trustline_asset %}
The asset of the trustline.
{% enddocs %}

{% docs value %}
The value for a data entry in manage data operations.
{% enddocs %}

{% docs clear_flags %}
Array of numeric values of flags cleared for a trustline.
{% enddocs %}

{% docs clear_flags_s %}
Array of string values of flags cleared for a trustline.
{% enddocs %}

{% docs destination_min %}
The minimum amount to be received in destination asset.
{% enddocs %}

{% docs bump_to %}
The new desired value of the source account's sequence number.
{% enddocs %}

{% docs sponsored_id %}
The account address of the account being sponsored.
{% enddocs %}

{% docs begin_sponsor %}
The account address that initiated the sponsorship.
{% enddocs %}

{% docs authorize_to_maintain_liabilities %}
Indicates whether the trustline is authorized for maintaining liabilities.
{% enddocs %}

{% docs clawback_enabled %}
Indicates whether the asset can be clawed back by the issuer.
{% enddocs %}

{% docs reserve_a_asset_type %}
The type of the first reserve asset.
{% enddocs %}

{% docs reserve_a_asset_id %}
The identifier of the first reserve asset.
{% enddocs %}

{% docs reserve_a_asset_code %}
The code of the first reserve asset.
{% enddocs %}

{% docs reserve_a_asset_issuer %}
The issuer of the first reserve asset.
{% enddocs %}

{% docs reserve_a_max_amount %}
Maximum amount for first reserve asset deposit.
{% enddocs %}

{% docs reserve_a_deposit_amount %}
Actual amount deposited for first reserve asset.
{% enddocs %}

{% docs reserve_b_asset_type %}
The type of the second reserve asset.
{% enddocs %}

{% docs reserve_b_asset_id %}
The identifier of the second reserve asset.
{% enddocs %}

{% docs reserve_b_asset_code %}
The code of the second reserve asset.
{% enddocs %}

{% docs reserve_b_asset_issuer %}
The issuer of the second reserve asset.
{% enddocs %}

{% docs reserve_b_max_amount %}
Maximum amount for second reserve asset deposit.
{% enddocs %}

{% docs reserve_b_deposit_amount %}
Actual amount deposited for second reserve asset.
{% enddocs %}

{% docs min_price %}
Minimum exchange rate for deposit operation.
{% enddocs %}

{% docs max_price %}
Maximum exchange rate for deposit operation.
{% enddocs %}

{% docs shares_received %}
Number of pool shares received for deposit.
{% enddocs %}

{% docs reserve_a_min_amount %}
Minimum withdrawal amount for first reserve asset.
{% enddocs %}

{% docs reserve_b_min_amount %}
Minimum withdrawal amount for second reserve asset.
{% enddocs %}

{% docs shares %}
Number of shares withdrawn from pool.
{% enddocs %}

{% docs reserve_a_withdraw_amount %}
Actual withdrawal amount for first reserve asset.
{% enddocs %}

{% docs reserve_b_withdraw_amount %}
Actual withdrawal amount for second reserve asset.
{% enddocs %}

{% docs op_source_account %}
Source account for the operation.
{% enddocs %}

{% docs op_source_account_muxed %}
Muxed source account for the operation.
{% enddocs %}

{% docs transaction_id %}
The transaction identifier containing this operation.
{% enddocs %}

{% docs txn_account %}
Account executing the transaction.
{% enddocs %}

{% docs account_sequence %}
Sequence number for the account.
{% enddocs %}

{% docs max_fee %}
Maximum fee for the transaction.
{% enddocs %}

{% docs txn_operation_count %}
Number of operations in the transaction.
{% enddocs %}

{% docs txn_created_at %}
Creation timestamp of the transaction.
{% enddocs %}

{% docs memo_type %}
Type of memo attached to transaction.
{% enddocs %}

{% docs memo %}
Memo content of the transaction.
{% enddocs %}

{% docs time_bounds %}
Time bounds for transaction validity.
{% enddocs %}

{% docs fee_account %}
Account charged the fee.
{% enddocs %}

{% docs new_max_fee %}
Updated maximum fee.
{% enddocs %}

{% docs account_muxed %}
Muxed account identifier.
{% enddocs %}

{% docs fee_account_muxed %}
Muxed fee account identifier.
{% enddocs %}

{% docs ledger_operation_count %}
Number of operations in the ledger.
{% enddocs %}

{% docs ledger_bounds %}
Bounds of the ledger.
{% enddocs %}

{% docs min_account_sequence %}
Minimum account sequence number.
{% enddocs %}

{% docs min_account_sequence_age %}
Minimum age of account sequence.
{% enddocs %}

{% docs min_account_sequence_ledger_gap %}
Minimum gap in ledger sequences.
{% enddocs %}

{% docs extra_signers %}
Additional transaction signers.
{% enddocs %}

{% docs asset_balance_changes %}
Changes in asset balances.
{% enddocs %}

{% docs parameters %}
Parameters for contract function calls.
{% enddocs %}

{% docs parameters_decoded %}
Decoded contract parameters.
{% enddocs %}

{% docs function %}
Function type for invoke_host_function.
{% enddocs %}

{% docs address %}
Address for Soroban contract creation.
{% enddocs %}

{% docs soroban_operation_type %}
Type of Soroban operation.
{% enddocs %}

{% docs extend_to %}
Ledger extension target.
{% enddocs %}

{% docs contract_id %}
Soroban contract identifier.
{% enddocs %}

{% docs contract_code_hash %}
Hash of Soroban contract code.
{% enddocs %}

{% docs resource_fee %}
Fee for resource usage.
{% enddocs %}

{% docs soroban_resources_instructions %}
Instructions for Soroban resources.
{% enddocs %}

{% docs soroban_resources_read_bytes %}
Bytes read by Soroban resources.
{% enddocs %}

{% docs soroban_resources_write_bytes %}
Bytes written by Soroban resources.
{% enddocs %}

{% docs transaction_result_code %}
Result code of the transaction.
{% enddocs %}

{% docs inclusion_fee_bid %}
Bid for inclusion fee.
{% enddocs %}

{% docs inclusion_fee_charged %}
Actual inclusion fee charged.
{% enddocs %}

{% docs resource_fee_refund %}
Refund of resource fee.
{% enddocs %}

{% docs op_application_order %}
Order of operation application.
{% enddocs %}

{% docs txn_application_order %}
Order of transaction application.
{% enddocs %}

{% docs operations_id %}
Unique identifier for the operation record.
{% enddocs %}

{% docs details_json %}
JSON record containing operation-specific details.
{% enddocs %}

{# Asset-specific fields #}

{% docs asset_id %}
The Farm Hash encoding of Asset Code + Asset Issuer + Asset Type. This field is optimized for cross table joins since integer joins are less expensive than the original asset id components.
{% enddocs %}

{% docs dim_assets_id %}
{{ doc("pk") }}
{% enddocs %}

{# Trade-specific fields #}

{% docs history_operation_id %}
The operation id associated with the executed trade. The total amount traded in an operation can be broken up into multiple smaller trades spread across multiple orders by multiple parties.
{% enddocs %}

{% docs order %}
The sequential number assigned the portion of a trade that is executed within a operation. The history_operation_id and order number together represent a unique trade segment.
{% enddocs %}

{% docs selling_account_address %}
The account address of the selling party.
{% enddocs %}

{% docs selling_amount %}
The amount of sold asset that was moved from the seller account to the buyer account, reported in terms of the sold amount.
{% enddocs %}

{% docs buying_account_address %}
The account address of the buying party.
{% enddocs %}

{% docs buying_amount %}
The amount of purchased asset that was moved from the seller account into the buying account, reported in terms of the bought asset.
{% enddocs %}

{% docs price_n %}
The price ratio of the sold asset: bought asset. When taken with price_d, the price can be calculated by price_n/price_d.
{% enddocs %}

{% docs price_d %}
The price ratio of the sold asset: bought asset. When taken with price_n, the price can be calculated by price_n/price_d.
{% enddocs %}

{% docs selling_offer_id %}
The offer ID in the orderbook of the selling offer. If this offer was immediately and fully consumed, this will be a synthetic ID.
{% enddocs %}

{% docs buying_offer_id %}
The offer ID in the orderbook of the buying offer. If this offer was immediately and fully consumed, this will be a synthetic ID.
{% enddocs %}

{% docs selling_liquidity_pool_id %}
The unique identifier for a liquidity pool if the trade was executed against a liquidity pool instead of the orderbook.
{% enddocs %}

{% docs liquidity_pool_fee %}
The percentage fee (in basis points) of the total fee collected by the liquidity pool for executing the trade.
{% enddocs %}

{% docs trade_type %}
Indicates whether the trade was executed against the orderbook (decentralized exchange) or liquidity pool.
{% enddocs %}

{% docs rounding_slippage %}
Applies to liquidity pool trades only. With fractional amounts of an asset traded, the network must round a fraction to the nearest whole number. This can cause the trade to "slip" price by a percentage compared with the original offer. Rounding slippage reports the percentage that dust trades slip before executing.
{% enddocs %}

{% docs seller_is_exact %}
Indicates whether the buying or selling party trade was impacted by rounding slippage. If true, the buyer was impacted. If false, the seller was impacted.
{% enddocs %}

{% docs trades_id %}
Surrogate key generated from history_operation_id and order.
{% enddocs %}

{% docs inner_transaction_hash %}
A transaction hash of a transaction wrapped with its signatures for fee-bump transactions
{% enddocs %}

{% docs tx_envelope %}
A base-64 encoded XDR blob of the tx envelope (transaction and its signatures)
{% enddocs %}

{% docs tx_result %}
A base-64 encoded XDR blob of the tx result
{% enddocs %}

{% docs tx_meta %}
A base-64 encoded XDR blob of the tx meta
{% enddocs %}

{% docs tx_fee_meta %}
A base-64 encoded XDR blob of the tx fee meta
{% enddocs %}

{% docs non_refundable_resource_fee_charged %}
The amount of the resource fee charged for the transaction that is non-refundable.
{% enddocs %}

{% docs refundable_resource_fee_charged %}
The amount of the resource fee charged for the transaction that is refundable.
{% enddocs %}

{% docs rent_fee_charged %}
The fee charged for renting resources on the network, such as storage space for data.
{% enddocs %}

{% docs tx_signers %}
The public keys of the signers who authorized the transaction.
{% enddocs %}

{% docs refundable_fee %}
The portion of the transaction fee that is refundable under certain conditions.
{% enddocs %}