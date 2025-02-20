--depends_on: {{ ref('silver__trades') }}
{{ config(
    materialized = 'incremental',
    unique_key = ["history_operation_id", "trade_order"],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE','ledger_closed_at::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(selling_account_address,selling_asset_code,selling_asset_issuer,buying_account_address,buying_asset_code,buying_asset_issuer);",
    tags = ['scheduled_core']
) }}

SELECT
    history_operation_id,
    "order" AS trade_order,
    ledger_closed_at,
    ledger_closed_at AS block_timestamp,
    selling_account_address,
    selling_asset_code,
    selling_asset_issuer,
    selling_asset_type,
    selling_asset_id,
    selling_amount,
    buying_account_address,
    buying_asset_code,
    buying_asset_issuer,
    buying_asset_type,
    buying_asset_id,
    buying_amount,
    price_n,
    price_d,
    selling_offer_id,
    buying_offer_id,
    batch_id,
    batch_run_date,
    batch_insert_ts,
    selling_liquidity_pool_id,
    liquidity_pool_fee,
    trade_type,
    rounding_slippage,
    seller_is_exact,
    {{ dbt_utils.generate_surrogate_key(
        ['history_operation_id','trade_order']
    ) }} AS fact_trades_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ ref('silver__trades') }}

{% if is_incremental() %}
WHERE
    modified_timestamp > (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
