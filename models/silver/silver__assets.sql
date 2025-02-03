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
{% set max_bat_query %}

SELECT
    MAX(batch_insert_ts) AS batch_insert_ts
FROM
    {{ this }}

    {% endset %}
    {% set max_batch = run_query(max_bat_query) [0] [0] %}
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
        id :: FLOAT AS id,
        asset_type :: STRING AS asset_type,
        asset_code :: STRING AS asset_code,
        asset_issuer :: STRING AS asset_issuer,
        batch_run_date :: datetime AS batch_run_date,
        batch_id :: STRING AS batch_id,
        batch_insert_ts :: datetime AS batch_insert_ts,
        asset_id :: INT AS asset_id
    FROM
        {# {% if is_incremental() %}
        {{ ref('bronze__assets') }}
    {% else %}
        {{ ref('bronze__assets_FR') }}
    {% endif %}

    #}
    {{ source(
        'bronze_streamline',
        'history_assets'
    ) }}

{% if is_incremental() %}
WHERE
    partition_id >= '{{ max_part }}'
    AND batch_insert_ts > '{{ max_batch }}'
{% endif %}

qualify ROW_NUMBER() over (
    PARTITION BY asset_id
    ORDER BY
        batch_insert_ts DESC
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
    {{ dbt_utils.generate_surrogate_key(
        ['asset_id']
    ) }} AS assets_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    pre_final
