-- depends_on: {{ ref('silver__ledgers') }}
{{ config(
    materialized = 'incremental',
    unique_key = ['sequence'],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    cluster_by = ['block_timestamp::DATE','closed_at::DATE'],
    merge_exclude_columns = ['inserted_timestamp'],
    tags = ['scheduled_core']
) }}

SELECT
    SEQUENCE,
    ledger_hash,
    previous_ledger_hash,
    transaction_count,
    operation_count,
    closed_at,
    closed_at AS block_timestamp,
    id,
    total_coins,
    fee_pool,
    base_fee,
    base_reserve,
    max_tx_set_size,
    protocol_version,
    successful_transaction_count,
    failed_transaction_count,
    tx_set_operation_count,
    batch_id,
    batch_run_date,
    batch_insert_ts,
    soroban_fee_write_1kb,
    node_id,
    signature,
    total_byte_size_of_bucket_list,
    {{ dbt_utils.generate_surrogate_key(
        ['sequence']
    ) }} AS fact_ledgers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ ref('silver__ledgers') }}

{% if is_incremental() %}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
