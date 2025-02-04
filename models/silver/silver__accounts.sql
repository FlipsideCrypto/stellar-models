-- depends_on: {{ ref('bronze__accounts') }}
{{ config(
    materialized = 'incremental',
    unique_key = ["account_id","closed_at"],
    incremental_predicates = ["dynamic_range_predicate", "partition_id::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['closed_at::DATE','partition_id','modified_timestamp::DATE'],
    tags = ['scheduled_core'],
) }}

{% if execute %}

{% if is_incremental() %}
{% set max_is_query %}

SELECT
    MAX(_inserted_timestamp) AS _inserted_timestamp,
    MAX(partition_gte_id) AS partition__gte_id
FROM
    {{ this }}

    {% endset %}
    {% set result = run_query(max_is_query) %}
    {% set max_is = result [0] [0] %}
    {% set max_part = result [0] [1] %}
{% endif %}
{% endif %}

WITH pre_final AS (
    SELECT
        partition_id,
        partition_gte_id,
        VALUE :account_id :: STRING AS account_id,
        TRY_CAST(
            VALUE :balance :: STRING AS FLOAT
        ) AS balance,
        TRY_CAST(
            VALUE :buying_liabilities :: STRING AS FLOAT
        ) AS buying_liabilities,
        TRY_CAST(
            VALUE :selling_liabilities :: STRING AS FLOAT
        ) AS selling_liabilities,
        VALUE :sequence_number :: INTEGER AS sequence_number,
        VALUE :num_subentries :: INTEGER AS num_subentries,
        VALUE :inflation_destination :: STRING AS inflation_destination,
        VALUE :flags :: INTEGER AS flags,
        VALUE :home_domain :: STRING AS home_domain,
        VALUE :master_weight :: INTEGER AS master_weight,
        VALUE :threshold_low :: INTEGER AS threshold_low,
        VALUE :threshold_medium :: INTEGER AS threshold_medium,
        VALUE :threshold_high :: INTEGER AS threshold_high,
        VALUE :last_modified_ledger :: INTEGER AS last_modified_ledger,
        VALUE :ledger_entry_change :: INTEGER AS ledger_entry_change,
        VALUE :deleted :: BOOLEAN AS deleted,
        TO_TIMESTAMP(
            VALUE :batch_run_date :: INT,
            6
        ) AS batch_run_date,
        VALUE: batch_id :: STRING AS batch_id,
        TO_TIMESTAMP(
            VALUE :batch_insert_ts :: INT,
            6
        ) AS batch_insert_ts,
        VALUE :sponsor :: STRING AS sponsor,
        VALUE :num_sponsored :: INTEGER AS num_sponsored,
        VALUE :num_sponsoring :: INTEGER AS num_sponsoring,
        VALUE :sequence_ledger :: INTEGER AS sequence_ledger,
        TO_TIMESTAMP(
            VALUE :sequence_time :: INT,
            6
        ) AS sequence_time,
        TO_TIMESTAMP(
            VALUE :closed_at :: INT,
            6
        ) AS closed_at,
        TO_TIMESTAMP(
            VALUE :ledger_sequence :: INT,
            6
        ) AS ledger_sequence,
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
