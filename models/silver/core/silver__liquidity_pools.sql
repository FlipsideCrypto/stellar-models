-- depends_on: {{ ref('bronze__liquidity_pools') }}
{{ config(
    materialized = 'incremental',
    unique_key = ["liquidity_pool_id","closed_at"],
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
        liquidity_pool_id,
        VALUE :TYPE :: STRING AS TYPE,
        VALUE :fee :: INTEGER AS fee,
        VALUE :trustline_count :: INTEGER AS trustline_count,
        VALUE :pool_share_count :: FLOAT AS pool_share_count,
        VALUE :asset_a_type :: STRING AS asset_a_type,
        VALUE :asset_a_code :: STRING AS asset_a_code,
        VALUE :asset_a_issuer :: STRING AS asset_a_issuer,
        VALUE :asset_a_id :: INTEGER AS asset_a_id,
        VALUE :asset_a_amount :: FLOAT AS asset_a_amount,
        VALUE :asset_b_type :: STRING AS asset_b_type,
        VALUE :asset_b_code :: STRING AS asset_b_code,
        VALUE :asset_b_issuer :: STRING AS asset_b_issuer,
        VALUE :asset_b_id :: INTEGER AS asset_b_id,
        VALUE :asset_b_amount :: FLOAT AS asset_b_amount,
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
        TO_TIMESTAMP(
            VALUE :closed_at :: INT,
            6
        ) AS closed_at,
        VALUE :ledger_sequence :: INTEGER AS ledger_sequence,
        _inserted_timestamp
    FROM

{% if is_incremental() %}
{{ ref('bronze__liquidity_pools') }}
{% else %}
    {{ ref('bronze__liquidity_pools_FR') }}
{% endif %}

{% if is_incremental() %}
WHERE
    partition_gte_id >= '{{ max_part }}'
    AND _inserted_timestamp > '{{ max_is }}'
{% endif %}

qualify ROW_NUMBER() over (
    PARTITION BY liquidity_pool_id,
    closed_at
    ORDER BY
        batch_insert_ts DESC,
        _inserted_timestamp DESC
) = 1
)
SELECT
    partition_id,
    partition_gte_id,
    liquidity_pool_id :: STRING AS liquidity_pool_id,
    TYPE,
    fee,
    trustline_count,
    pool_share_count,
    asset_a_type,
    asset_a_code,
    asset_a_issuer,
    asset_a_id,
    asset_a_amount,
    asset_b_type,
    asset_b_code,
    asset_b_issuer,
    asset_b_id,
    asset_b_amount,
    last_modified_ledger,
    ledger_entry_change,
    deleted,
    batch_id,
    batch_run_date,
    batch_insert_ts,
    closed_at,
    ledger_sequence,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['liquidity_pool_id','closed_at']
    ) }} AS liquidity_pools_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    pre_final
