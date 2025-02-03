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
{% set max_is_query %}

SELECT
    MAX(_inserted_timestamp) AS _inserted_timestamp
FROM
    {{ this }}

    {% endset %}
    {% set max_is = run_query(max_is_query) [0] [0] %}
    {% set max_part_query %}
SELECT
    MAX(partition_gte_id) AS partition__gte_id
FROM
    {{ this }}

    {% endset %}
    {% set max_part = run_query(max_part_query) [0] [0] %}
{% endif %}
{% endif %}

WITH pre_final AS (
    SELECT
        partition_id,
        partition_gte_id,
        account_id :: STRING AS account_id,
        balance :: FLOAT AS balance,
        buying_liabilities :: FLOAT AS buying_liabilities,
        selling_liabilities :: FLOAT AS selling_liabilities,
        sequence_number :: INTEGER AS sequence_number,
        num_subentries :: INTEGER AS num_subentries,
        inflation_destination :: STRING AS inflation_destination,
        flags :: INTEGER AS flags,
        home_domain :: STRING AS home_domain,
        master_weight :: INTEGER AS master_weight,
        threshold_low :: INTEGER AS threshold_low,
        threshold_medium :: INTEGER AS threshold_medium,
        threshold_high :: INTEGER AS threshold_high,
        last_modified_ledger :: INTEGER AS last_modified_ledger,
        ledger_entry_change :: INTEGER AS ledger_entry_change,
        deleted :: BOOLEAN AS deleted,
        batch_id :: STRING AS batch_id,
        batch_run_date :: TIMESTAMP AS batch_run_date,
        batch_insert_ts :: TIMESTAMP AS batch_insert_ts,
        sponsor :: STRING AS sponsor,
        num_sponsored :: INTEGER AS num_sponsored,
        num_sponsoring :: INTEGER AS num_sponsoring,
        sequence_ledger :: INTEGER AS sequence_ledger,
        sequence_time :: TIMESTAMP AS sequence_time,
        closed_at :: TIMESTAMP AS closed_at,
        ledger_sequence :: INTEGER AS ledger_sequence,
        _inserted_timestamp
    FROM

{% if is_incremental() %}
{{ ref('bronze__accounts') }}
{% else %}
    {{ ref('bronze__accounts_FR') }}
{% endif %}

{% if is_incremental() %}
WHERE
    partition_gte_id >= '{{ max_part }}'
    AND _inserted_timestamp > '{{ max_is }}'
{% endif %}

qualify ROW_NUMBER() over (
    PARTITION BY account_id,
    closed_at
    ORDER BY
        batch_insert_ts DESC,
        _inserted_timestamp DESC
) = 1
)
SELECT
    partition_id,
    partition_gte_id,
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
