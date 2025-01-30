-- depends_on: {{ ref('silver__ledgers') }}
{{ config(
    materialized = 'incremental',
    unique_key = ['fact_ledgers_id'],
    incremental_predicates = ['DBT_INTERNAL_DEST.closed_at::DATE >= (select min(closed_at::DATE) from ' ~ generate_tmp_view_name(this) ~ ')'],
    cluster_by = ['closed_at::DATE'],
    merge_exclude_columns = ['inserted_timestamp'],
    tags = ['core']
) }}

SELECT
    sequence,
    ledger_hash,
    previous_ledger_hash,
    transaction_count,
    operation_count,
    closed_at,
    id,
    total_coins,
    fee_pool,
    base_fee,
    base_reserve,
    max_tx_set_size,
    protocol_version,
    ledger_header,
    successful_transaction_count,
    failed_transaction_count,
    tx_set_operation_count,
    soroban_fee_write_1kb,
    node_id,
    signature,
    total_byte_size_of_bucket_list,
    {{ dbt_utils.generate_surrogate_key(
        ['sequence']
    ) }} AS fact_ledgers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
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
