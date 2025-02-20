-- depends_on: {{ ref('silver__liquidity_pools') }}
{{ config(
    materialized = 'incremental',
    unique_key = ["liquidity_pool_id", "closed_at"],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE','closed_at::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(liquidity_pool_id,asset_a_issuer,asset_b_issuer,asset_a_code,asset_b_code);",
    tags = ['scheduled_core']
) }}

SELECT
    liquidity_pool_id,
    closed_at,
    closed_at AS block_timestamp,
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
    ledger_sequence,
    {{ dbt_utils.generate_surrogate_key(
        ['liquidity_pool_id', 'closed_at']
    ) }} AS fact_liquidity_pools_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ ref('silver__liquidity_pools') }}

{% if is_incremental() %}
WHERE
    modified_timestamp > (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}

qualify ROW_NUMBER() over (
    PARTITION BY liquidity_pool_id,
    closed_at
    ORDER BY
        _inserted_timestamp DESC
) = 1
