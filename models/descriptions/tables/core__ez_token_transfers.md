{% docs core__ez_token_transfers %} The token transfers raw table contains the SEP-41 compliant event stream from the token transfer processor. This table's purpose is to track the token value movement on the stellar network in the form of transfer, mint, burn, clawback, and fee events.

transfer, mint, burn, and clawback events are emitted at the operation grain. fee events are emitted at the transaction grain because there is no individual fee per operation.

fee events can be negative in the event of a refund. The final fee paid (intial fee + refund) will always be positive. More information about fee refunds can be found here.

Note that the events within this table are a subset of the events in the history_contract_events table. {% enddocs %}