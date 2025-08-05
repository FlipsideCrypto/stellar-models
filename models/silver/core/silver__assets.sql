-- depends_on: {{ ref('bronze__assets') }}
{{ config(
    materialized = 'incremental',
    unique_key = ["asset_issuer","asset_code"],
    incremental_predicates = ["dynamic_range_predicate", "partition_id::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['partition_id','modified_timestamp::DATE'],
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
        VALUE :id :: FLOAT AS id,
        VALUE :asset_type :: STRING AS asset_type,
        VALUE :asset_code :: STRING AS asset_code,
        VALUE :asset_issuer :: STRING AS asset_issuer,
        TO_TIMESTAMP(
            VALUE :batch_run_date :: INT,
            6
        ) AS batch_run_date,
        VALUE: batch_id :: STRING AS batch_id,
        TO_TIMESTAMP(
            VALUE :batch_insert_ts :: INT,
            6
        ) AS batch_insert_ts,
        VALUE :asset_id :: INT AS asset_id,
        _inserted_timestamp
    FROM

{% if is_incremental() %}
{{ ref('bronze__assets') }}
{% else %}
    {{ ref('bronze__assets_FR') }}
{% endif %}

{% if is_incremental() %}
WHERE
    partition_gte_id >= '{{ max_part }}'
    AND _inserted_timestamp > '{{ max_is }}'
{% endif %}

qualify ROW_NUMBER() over (
    PARTITION BY asset_issuer,
    asset_code
    ORDER BY
        batch_insert_ts DESC,
        _inserted_timestamp DESC
) = 1
)
SELECT
    partition_id,
    partition_gte_id,
    id,
    asset_type,
    asset_code,
    asset_issuer,
    batch_run_date,
    batch_id,
    batch_insert_ts,
    asset_id,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['asset_id']
    ) }} AS assets_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    pre_final
