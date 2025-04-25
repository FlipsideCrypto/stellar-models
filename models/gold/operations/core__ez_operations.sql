-- depends_on: {{ ref('core__fact_operations') }}
-- depends_on: {{ ref('core__fact_transactions') }}
-- depends_on: {{ ref('core__fact_ledgers') }}
{{ config(
    materialized = 'incremental',
    unique_key = ['op_id'],
    cluster_by = ['block_timestamp::DATE', 'closed_at::DATE', 'type_string'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(op_source_account,asset,asset_code,asset_issuer,buying_asset_code,buying_asset_issuer,selling_asset_code,selling_asset_issuer,tx_id,transaction_hash,ledger_sequence);",
    tags = ['scheduled_core']
) }}

{% if execute %}

{% if is_incremental() %}
{% set max_is_query %}

SELECT
    MAX(modified_timestamp) AS modified_timestamp
FROM
    {{ this }}

    {% endset %}
    {% set result = run_query(max_is_query) %}
    {% set max_mod = result [0] [0] %}
    {% set min_block_query %}
SELECT
    MIN(block_timestamp) AS block_timestamp
FROM
    (
        SELECT
            MIN(block_timestamp) AS block_timestamp
        FROM
            {{ ref('core__fact_operations') }}
        WHERE
            modified_timestamp >= '{{max_mod}}'
        UNION ALL
        SELECT
            MIN(block_timestamp) AS block_timestamp
        FROM
            {{ ref('core__fact_transactions') }}
        WHERE
            modified_timestamp >= '{{max_mod}}'
        UNION ALL
        SELECT
            MIN(block_timestamp) AS block_timestamp
        FROM
            {{ ref('core__fact_ledgers') }}
        WHERE
            modified_timestamp >= '{{max_mod}}'
    ) {% endset %}
    {% set min_bts = run_query(min_block_query) [0] [0] %}
    {% if not min_bts or min_bts == 'None' %}
        {% set min_bts = '2099-01-01 00:00:00' %}
    {% endif %}
{% endif %}
{% endif %}

WITH operations AS (
    SELECT
        id AS op_id,
        source_account AS op_source_account,
        op_source_account_muxed,
        account AS op_account_id,
        closed_at,
        block_timestamp,
        amount,
        asset,
        asset_code,
        asset_issuer,
        asset_type,
        authorize,
        COALESCE(
            balance_id,
            claimable_balance_id
        ) AS balance_id,
        claimant,
        claimant_muxed,
        claimant_muxed_id,
        claimants,
        data_account_id,
        data_name,
        buying_asset_code,
        buying_asset_issuer,
        buying_asset_type,
        from_account,
        from_muxed,
        from_muxed_id,
        funder,
        funder_muxed,
        funder_muxed_id,
        high_threshold,
        home_domain,
        inflation_dest,
        into_account,
        into_muxed,
        into_muxed_id,
        limit_amount,
        low_threshold,
        master_key_weight,
        med_threshold,
        NAME,
        offer_id,
        path,
        price,
        price_r,
        selling_asset_code,
        selling_asset_issuer,
        selling_asset_type,
        set_flags,
        set_flags_s,
        signer_account_id,
        signer_key,
        signer_weight,
        source_amount,
        source_asset_code,
        source_asset_issuer,
        source_asset_type,
        source_max,
        starting_balance,
        to_account,
        to_muxed,
        to_muxed_id,
        trustee,
        trustee_muxed,
        trustee_muxed_id,
        trustor,
        trustor_muxed,
        trustor_muxed_id,
        trustline_account_id,
        trustline_asset,
        VALUE,
        clear_flags,
        clear_flags_s,
        destination_min,
        bump_to,
        sponsor,
        sponsored_id,
        begin_sponsor,
        begin_sponsor_muxed,
        begin_sponsor_muxed_id,
        authorize_to_maintain_liabilities,
        clawback_enabled,
        liquidity_pool_id,
        reserve_a_asset_type,
        reserve_a_asset_code,
        reserve_a_asset_issuer,
        reserve_a_max_amount,
        reserve_a_deposit_amount,
        reserve_b_asset_type,
        reserve_b_asset_code,
        reserve_b_asset_issuer,
        reserve_b_max_amount,
        reserve_b_deposit_amount,
        min_price,
        min_price_r,
        max_price,
        max_price_r,
        shares_received,
        reserve_a_min_amount,
        reserve_b_min_amount,
        shares,
        reserve_a_withdraw_amount,
        reserve_b_withdraw_amount,
        transaction_id AS tx_id,
        TYPE,
        type_string,
        batch_id,
        batch_run_date,
        asset_balance_changes,
        PARAMETERS,
        parameters_decoded,
        FUNCTION,
        address,
        soroban_operation_type,
        extend_to,
        contract_id,
        contract_code_hash,
        operation_result_code,
        operation_trace_code,
        details_json,
        modified_timestamp
    FROM
        {{ ref('core__fact_operations') }}

{% if is_incremental() %}
WHERE
    block_timestamp >= '{{min_bts}}'
{% endif %}
),
transactions AS (
    SELECT
        id AS tx_id,
        transaction_hash,
        ledger_sequence,
        account AS tx_account,
        account_sequence,
        max_fee,
        operation_count AS tx_operation_count,
        created_at AS tx_created_at,
        memo_type,
        memo,
        time_bounds,
        SUCCESSFUL,
        fee_charged,
        fee_account,
        new_max_fee,
        account_muxed,
        fee_account_muxed,
        ledger_bounds,
        min_account_sequence,
        min_account_sequence_age,
        min_account_sequence_ledger_gap,
        extra_signers,
        batch_id,
        batch_run_date,
        resource_fee,
        soroban_resources_instructions,
        soroban_resources_read_bytes,
        soroban_resources_write_bytes,
        transaction_result_code,
        inclusion_fee_bid,
        inclusion_fee_charged,
        resource_fee_refund,
        non_refundable_resource_fee_charged,
        refundable_resource_fee_charged,
        rent_fee_charged,
        tx_signers,
        refundable_fee
    FROM
        {{ ref('core__fact_transactions') }}

{% if is_incremental() %}
WHERE
    block_timestamp >= '{{min_bts}}'
{% endif %}
),
ledgers AS (
    SELECT
        SEQUENCE AS ledger_sequence,
        ledger_hash,
        previous_ledger_hash,
        transaction_count,
        operation_count AS ledger_operation_count,
        id AS ledger_id,
        total_coins,
        fee_pool,
        base_fee,
        base_reserve,
        max_tx_set_size,
        protocol_version,
        successful_transaction_count,
        failed_transaction_count,
        soroban_fee_write_1kb,
        node_id,
        signature,
        total_byte_size_of_bucket_list,
        batch_id,
        batch_run_date
    FROM
        {{ ref('core__fact_ledgers') }}

{% if is_incremental() %}
WHERE
    block_timestamp >= '{{min_bts}}'
{% endif %}
)
SELECT
    o.op_id,
    o.op_source_account,
    o.op_source_account_muxed,
    o.op_account_id,
    o.amount,
    o.asset,
    o.asset_code,
    o.asset_issuer,
    o.asset_type,
    o.authorize,
    o.balance_id,
    o.claimant,
    o.claimant_muxed,
    o.claimant_muxed_id,
    o.claimants,
    o.data_account_id,
    o.data_name,
    o.buying_asset_code,
    o.buying_asset_issuer,
    o.buying_asset_type,
    o.from_account,
    o.from_muxed,
    o.from_muxed_id,
    o.funder,
    o.funder_muxed,
    o.funder_muxed_id,
    o.high_threshold,
    o.home_domain,
    o.inflation_dest,
    o.into_account,
    o.into_muxed,
    o.into_muxed_id,
    o.limit_amount,
    o.low_threshold,
    o.master_key_weight,
    o.med_threshold,
    o.name,
    o.offer_id,
    o.path,
    o.price,
    o.price_r,
    o.selling_asset_code,
    o.selling_asset_issuer,
    o.selling_asset_type,
    o.set_flags,
    o.set_flags_s,
    o.signer_account_id,
    o.signer_key,
    o.signer_weight,
    o.source_amount,
    o.source_asset_code,
    o.source_asset_issuer,
    o.source_asset_type,
    o.source_max,
    o.starting_balance,
    o.to_account,
    o.to_muxed,
    o.to_muxed_id,
    o.trustee,
    o.trustee_muxed,
    o.trustee_muxed_id,
    o.trustor,
    o.trustor_muxed,
    o.trustor_muxed_id,
    o.trustline_account_id,
    o.trustline_asset,
    o.value,
    o.clear_flags,
    o.clear_flags_s,
    o.destination_min,
    o.bump_to,
    o.sponsor,
    o.sponsored_id,
    o.begin_sponsor,
    o.begin_sponsor_muxed,
    o.begin_sponsor_muxed_id,
    o.authorize_to_maintain_liabilities,
    o.clawback_enabled,
    o.liquidity_pool_id,
    o.reserve_a_asset_type,
    o.reserve_a_asset_code,
    o.reserve_a_asset_issuer,
    o.reserve_a_max_amount,
    o.reserve_a_deposit_amount,
    o.reserve_b_asset_type,
    o.reserve_b_asset_code,
    o.reserve_b_asset_issuer,
    o.reserve_b_max_amount,
    o.reserve_b_deposit_amount,
    o.min_price,
    o.min_price_r,
    o.max_price,
    o.max_price_r,
    o.shares_received,
    o.reserve_a_min_amount,
    o.reserve_b_min_amount,
    o.shares,
    o.reserve_a_withdraw_amount,
    o.reserve_b_withdraw_amount,
    o.tx_id,
    o.type,
    o.type_string,
    o.batch_id,
    o.batch_run_date,
    o.asset_balance_changes,
    o.parameters,
    o.parameters_decoded,
    o.function,
    o.address,
    o.soroban_operation_type,
    o.extend_to,
    o.contract_id,
    o.contract_code_hash,
    o.operation_result_code,
    o.operation_trace_code,
    o.details_json,
    o.closed_at,
    o.block_timestamp,
    t.transaction_hash,
    t.ledger_sequence,
    t.tx_account,
    t.account_sequence,
    t.max_fee,
    t.tx_operation_count,
    t.tx_created_at,
    t.memo_type,
    t.memo,
    t.time_bounds,
    t.successful,
    t.fee_charged,
    t.fee_account,
    t.new_max_fee,
    t.account_muxed,
    t.fee_account_muxed,
    t.ledger_bounds,
    t.min_account_sequence,
    t.min_account_sequence_age,
    t.min_account_sequence_ledger_gap,
    t.extra_signers,
    t.resource_fee,
    t.soroban_resources_instructions,
    t.soroban_resources_read_bytes,
    t.soroban_resources_write_bytes,
    t.transaction_result_code,
    t.inclusion_fee_bid,
    t.inclusion_fee_charged,
    t.resource_fee_refund,
    l.ledger_hash,
    l.previous_ledger_hash,
    l.transaction_count,
    l.ledger_operation_count,
    l.ledger_id,
    l.total_coins,
    l.fee_pool,
    l.base_fee,
    l.base_reserve,
    l.max_tx_set_size,
    l.protocol_version,
    l.successful_transaction_count,
    l.failed_transaction_count,
    l.soroban_fee_write_1kb,
    l.node_id,
    l.signature,
    l.total_byte_size_of_bucket_list,
    t.non_refundable_resource_fee_charged,
    t.refundable_resource_fee_charged,
    t.rent_fee_charged,
    t.tx_signers,
    t.refundable_fee,
    {{ dbt_utils.generate_surrogate_key(['op_id']) }} AS fact_operations_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    operations o
    LEFT JOIN transactions t
    ON o.tx_id = t.tx_id
    LEFT JOIN ledgers l
    ON t.ledger_sequence = l.ledger_sequence
