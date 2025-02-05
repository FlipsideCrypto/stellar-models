-- depends_on: {{ ref('silver__liquidity_pools') }}
{{ config(
    materialized = 'incremental',
    unique_key = ["liquidity_pool_id", "closed_at"],
    incremental_predicates = ["dynamic_range_predicate", "partition_id::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['closed_at::DATE', 'partition_id', 'modified_timestamp::DATE'],
    tags = ['core'],
) }}

SELECT
    liquidity_pool_id,
    type,
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
        ['liquidity_pool_id', 'closed_at']
    ) }} AS fact_liquidity_pools_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM 
    {{ ref('silver__liquidity_pools') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp > (SELECT MAX(_inserted_timestamp) FROM {{ this }})
{% endif %}

QUALIFY ROW_NUMBER() OVER (
    PARTITION BY liquidity_pool_id, closed_at
    ORDER BY _inserted_timestamp DESC
) = 1
