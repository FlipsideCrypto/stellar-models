-- depends_on: {{ ref('bronze__trust_lines') }}
{{ config(
    materialized = 'incremental',
    unique_key = "trust_lines_id",
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
        VALUE :asset_id :: bigint AS asset_id,
        VALUE :asset_type :: STRING AS asset_type,
        VALUE :asset_issuer :: STRING AS asset_issuer,
        VALUE :asset_code :: STRING AS asset_code,
        VALUE :liquidity_pool_id :: STRING AS liquidity_pool_id,
        VALUE :balance :: FLOAT AS balance,
        VALUE :trust_line_limit :: FLOAT AS trust_line_limit,
        VALUE :buying_liabilities :: FLOAT AS buying_liabilities,
        VALUE :selling_liabilities :: FLOAT AS selling_liabilities,
        VALUE :flags :: STRING AS flags,
        VALUE :last_modified_ledger :: bigint AS last_modified_ledger,
        VALUE :ledger_entry_change :: INT AS ledger_entry_change,
        VALUE :deleted :: BOOLEAN AS deleted,
        TO_TIMESTAMP(
            VALUE :closed_at :: INT,
            6
        ) AS closed_at,
        VALUE :ledger_sequence :: bigint AS ledger_sequence,
        VALUE :ledger_key :: STRING AS ledger_key,
        VALUE :sponsor :: STRING AS sponsor,
        VALUE :batch_id :: STRING AS batch_id,
        TO_TIMESTAMP(
            VALUE :batch_run_date :: INT,
            6
        ) AS batch_run_date,
        TO_TIMESTAMP(
            VALUE :batch_insert_ts :: INT,
            6
        ) AS batch_insert_ts,
        _inserted_timestamp
    FROM

{% if is_incremental() %}
{{ ref('bronze__trust_lines') }}
{% else %}
    {{ ref('bronze__trust_lines_FR') }}
{% endif %}

{% if is_incremental() %}
WHERE
    partition_gte_id >= '{{ max_part }}'
    AND _inserted_timestamp > '{{ max_is }}'
{% endif %}

qualify ROW_NUMBER() over (
    PARTITION BY account_id,
    asset_type,
    asset_issuer,
    asset_code,
    liquidity_pool_id,
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
    asset_id,
    asset_type,
    asset_issuer,
    asset_code,
    liquidity_pool_id,
    balance,
    trust_line_limit,
    buying_liabilities,
    selling_liabilities,
    flags,
    last_modified_ledger,
    ledger_entry_change,
    deleted,
    closed_at,
    ledger_sequence,
    ledger_key,
    sponsor,
    batch_id,
    batch_run_date,
    batch_insert_ts,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['account_id', 'asset_type', 'asset_issuer', 'asset_code', 'liquidity_pool_id', 'closed_at']
    ) }} AS trust_lines_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    pre_final
