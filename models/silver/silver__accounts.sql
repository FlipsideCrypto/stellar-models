-- depends_on: {{ ref('bronze__accounts') }}
{{ config(
    materialized = 'incremental',
    unique_key = ["account_id","closed_at"],
    incremental_predicates = ["dynamic_range_predicate", "partition_id::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['closed_at::DATE','partition_id','modified_timestamp::DATE'],
    full_refresh = false,
    tags = ['scheduled_core'],
) }}

{% if execute %}

{% if is_incremental() %}
{% set max_inserted_query %}

SELECT
    MAX(_inserted_timestamp) AS _inserted_timestamp
FROM
    {{ this }}

    {% endset %}
    {% set max_inserted_timestamp = run_query(max_inserted_query) [0] [0] %}
{% endif %}
{% endif %}

WITH pre_final AS (
    SELECT
        partition_id,
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
        batch_id,
        batch_run_date,
        batch_insert_ts,
        sponsor,
        num_sponsored,
        num_sponsoring,
        sequence_ledger,
        sequence_time,
        closed_at,
        ledger_sequence,
        _inserted_timestamp
    FROM

{% if is_incremental() %}
{{ ref('bronze__accounts') }}
{% else %}
    {{ ref('bronze__accounts_FR') }}
{% endif %}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= '{{ max_inserted_timestamp }}'
{% endif %}

qualify ROW_NUMBER() over (
    PARTITION BY account_id,
    closed_at
    ORDER BY
        _inserted_timestamp DESC
) = 1
)
SELECT
    partition_id,
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
    batch_id,
    batch_run_date,
    batch_insert_ts,
    sponsor,
    num_sponsored,
    num_sponsoring,
    sequence_ledger,
    sequence_time,
    closed_at,
    ledger_sequence,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['account_id','closed_at']
    ) }} AS accounts_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    pre_final
