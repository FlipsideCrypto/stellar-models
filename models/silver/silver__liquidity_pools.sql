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
        liquidity_pool_id,
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
        _inserted_timestamp
    FROM

{% if is_incremental() %}
{{ ref('bronze__liquidity_pools') }}
{% else %}
    {{ ref('bronze__liquidity_pools_FR') }}
{% endif %}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= '{{ max_inserted_timestamp }}'
{% endif %}

qualify ROW_NUMBER() over (
    PARTITION BY liquidity_pool_id,
    closed_at
    ORDER BY
        _inserted_timestamp DESC
) = 1
)
SELECT
    partition_id,
    liquidity_pool_id,
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
