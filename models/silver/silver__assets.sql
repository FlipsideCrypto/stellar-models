-- depends_on: {{ ref('bronze__assets') }}
{{ config(
    materialized = 'incremental',
    unique_key = "asset_id",
    incremental_predicates = ["dynamic_range_predicate", "partition_id::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['partition_id','modified_timestamp::DATE'],
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

{% if is_incremental() %}
{% set max_part_query %}
SELECT
    MAX(partition_id) AS partition_id
FROM
    {{ this }}

    {% endset %}
    {% set max_part = run_query(max_part_query) [0] [0] %}
{% endif %}
{% endif %}

WITH pre_final AS (
    SELECT
        partition_id,
        id :: flot AS id,
        asset_type :: STRING AS asset_type,
        asset_code :: STRING AS asset_code,
        asset_issuer :: STRING AS asset_issuer,
        batch_run_date :: datetime AS batch_run_date,
        batch_id :: STRING AS batch_id,
        batch_insert_ts :: datetime AS batch_insert_ts,
        asset_id :: INT AS asset_id,
        _inserted_timestamp
    FROM

{% if is_incremental() %}
{{ ref('bronze__assets') }}
{% else %}
    {{ ref('bronze__assets_FR') }}
{% endif %}

{% if is_incremental() %}
WHERE
    partition_id >= '{{ max_part }}'
    AND _inserted_timestamp > '{{ max_inserted_timestamp }}'
{% endif %}

qualify ROW_NUMBER() over (
    PARTITION BY asset_id
    ORDER BY
        batch_insert_ts DESC,
        _inserted_timestamp DESC
) = 1
)
SELECT
    partition_id,
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
