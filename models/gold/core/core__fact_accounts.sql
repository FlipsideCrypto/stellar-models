-- depends_on: {{ ref('silver__accounts') }}
{{ config(
    materialized = 'incremental',
    unique_key = ["account_id","closed_at"],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE','closed_at::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(account_id);",
    tags = ['scheduled_core']
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
    closed_at AS block_timestamp,
    ledger_sequence,
    {{ dbt_utils.generate_surrogate_key(['account_id', 'closed_at']) }} AS fact_accounts_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
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
