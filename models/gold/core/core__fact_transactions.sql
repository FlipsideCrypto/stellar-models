-- depends_on: {{ ref('silver__transactions') }}
{{ config(
    materialized = 'incremental',
    unique_key = ["id"],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE','closed_at::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(id,transaction_hash,account);",
    tags = ['scheduled_core']
) }}

SELECT
    id,
    transaction_hash,
    ledger_sequence,
    closed_at,
    closed_at AS block_timestamp,
    account,
    account_sequence,
    max_fee,
    operation_count,
    created_at,
    memo_type,
    memo,
    time_bounds,
    SUCCESSFUL,
    fee_charged,
    inner_transaction_hash,
    fee_account,
    new_max_fee,
    account_muxed,
    fee_account_muxed,
    batch_id,
    batch_run_date,
    batch_insert_ts,
    ledger_bounds,
    min_account_sequence,
    min_account_sequence_age,
    min_account_sequence_ledger_gap,
    tx_envelope,
    tx_result,
    tx_meta,
    tx_fee_meta,
    extra_signers,
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
    refundable_fee,
    {{ dbt_utils.generate_surrogate_key(
        ['id']
    ) }} AS fact_transactions_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ ref('silver__transactions') }}

{% if is_incremental() %}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
