-- depends_on: {{ ref('bronze__trades') }}
{{ config(
    materialized = 'incremental',
    unique_key = ['history_operation_id','"order"'],
    incremental_predicates = ["dynamic_range_predicate", "partition_id::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['ledger_closed_at::DATE','partition_id','modified_timestamp::DATE'],
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
        history_operation_id :: INTEGER AS history_operation_id,
        "order" :: INTEGER AS "order",
        ledger_closed_at :: TIMESTAMP AS ledger_closed_at,
        selling_account_address :: STRING AS selling_account_address,
        selling_asset_code :: STRING AS selling_asset_code,
        selling_asset_issuer :: STRING AS selling_asset_issuer,
        selling_asset_type :: STRING AS selling_asset_type,
        selling_asset_id :: INTEGER AS selling_asset_id,
        selling_amount :: FLOAT AS selling_amount,
        buying_account_address :: STRING AS buying_account_address,
        buying_asset_code :: STRING AS buying_asset_code,
        buying_asset_issuer :: STRING AS buying_asset_issuer,
        buying_asset_type :: STRING AS buying_asset_type,
        buying_asset_id :: INTEGER AS buying_asset_id,
        buying_amount :: FLOAT AS buying_amount,
        price_n :: INTEGER AS price_n,
        price_d :: INTEGER AS price_d,
        selling_offer_id :: INTEGER AS selling_offer_id,
        buying_offer_id :: INTEGER AS buying_offer_id,
        batch_id :: STRING AS batch_id,
        batch_run_date :: TIMESTAMP AS batch_run_date,
        batch_insert_ts :: TIMESTAMP AS batch_insert_ts,
        selling_liquidity_pool_id :: STRING AS selling_liquidity_pool_id,
        liquidity_pool_fee :: INTEGER AS liquidity_pool_fee,
        trade_type :: INTEGER AS trade_type,
        rounding_slippage :: INTEGER AS rounding_slippage,
        seller_is_exact :: BOOLEAN AS seller_is_exact,
        _inserted_timestamp
    FROM

{% if is_incremental() %}
{{ ref('bronze__trades') }}
{% else %}
    {{ ref('bronze__trades_FR') }}
{% endif %}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= '{{ max_inserted_timestamp }}'
{% endif %}

qualify ROW_NUMBER() over (
    PARTITION BY history_operation_id,
    "order"
    ORDER BY
        _inserted_timestamp DESC
) = 1
)
SELECT
    partition_id,
    history_operation_id,
    "order",
    ledger_closed_at,
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
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['history_operation_id','"order"']
    ) }} AS trades_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    pre_final
