-- depends_on: {{ ref('silver__transactions') }}
{{ config(
    materialized = 'incremental',
    unique_key = ["fact_trust_lines_id"],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE','closed_at::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(account_id,asset_id,asset_code);",
    tags = ['scheduled_core']
) }}

SELECT
    account_id,
    closed_at,
    closed_at AS block_timestamp,
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
    ledger_sequence,
    ledger_key,
    sponsor,
    batch_id,
    batch_run_date,
    batch_insert_ts,
    trust_lines_id AS fact_trust_lines_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ ref('silver__trust_lines') }}

{% if is_incremental() %}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
