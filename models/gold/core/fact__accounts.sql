{{ config(
    materialized = 'incremental',
    unique_key = ["account_id","closed_at"],
    incremental_predicates = ["dynamic_range_predicate", "partition_id::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['closed_at::DATE','partition_id','modified_timestamp::DATE'],
    tags = ['core']
) }}

SELECT
    account_id,
    balance,
    buying_liabilities,
    selling_liabilities,
    sequence_number,
    num_subentries,
    inflation_destination,
    flags,
    home_domain,
    master_weight,
    threshold_low,
    threshold_medium, 
    threshold_high,
    last_modified_ledger,
    ledger_entry_change,
    deleted,
    sponsor,
    num_sponsored,
    num_sponsoring,
    sequence_ledger,
    sequence_time,
    closed_at,
    ledger_sequence,
    {{ dbt_utils.generate_surrogate_key(['account_id', 'closed_at']) }} AS core_accounts_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM 
    {{ ref('silver__accounts') }}

{% if is_incremental() %}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}