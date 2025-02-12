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
        history_operation_id :: INTEGER AS history_operation_id,
        VALUE: "order" :: INTEGER AS "order",
        TO_TIMESTAMP(
            VALUE :ledger_closed_at :: INT,
            6
        ) AS ledger_closed_at,
        VALUE: selling_account_address :: STRING AS selling_account_address,
        VALUE: selling_asset_code :: STRING AS selling_asset_code,
        VALUE: selling_asset_issuer :: STRING AS selling_asset_issuer,
        VALUE: selling_asset_type :: STRING AS selling_asset_type,
        VALUE: selling_asset_id :: INTEGER AS selling_asset_id,
        VALUE: selling_amount :: FLOAT AS selling_amount,
        VALUE: buying_account_address :: STRING AS buying_account_address,
        VALUE: buying_asset_code :: STRING AS buying_asset_code,
        VALUE: buying_asset_issuer :: STRING AS buying_asset_issuer,
        VALUE: buying_asset_type :: STRING AS buying_asset_type,
        VALUE: buying_asset_id :: INTEGER AS buying_asset_id,
        VALUE: buying_amount :: FLOAT AS buying_amount,
        VALUE: price_n :: INTEGER AS price_n,
        VALUE: price_d :: INTEGER AS price_d,
        VALUE: selling_offer_id :: INTEGER AS selling_offer_id,
        VALUE: buying_offer_id :: INTEGER AS buying_offer_id,
        VALUE :batch_id :: STRING AS batch_id,
        TO_TIMESTAMP(
            VALUE :batch_run_date :: INT,
            6
        ) AS batch_run_date,
        TO_TIMESTAMP(
            VALUE :batch_insert_ts :: INT,
            6
        ) AS batch_insert_ts,
        VALUE: selling_liquidity_pool_id :: STRING AS selling_liquidity_pool_id,
        VALUE: liquidity_pool_fee :: INTEGER AS liquidity_pool_fee,
        VALUE: trade_type :: INTEGER AS trade_type,
        VALUE: rounding_slippage :: INTEGER AS rounding_slippage,
        VALUE: seller_is_exact :: BOOLEAN AS seller_is_exact,
        _inserted_timestamp
    FROM

{% if is_incremental() %}
{{ ref('bronze__trades') }}
{% else %}
    {{ ref('bronze__trades_FR') }}
{% endif %}

{% if is_incremental() %}
WHERE
    partition_gte_id >= '{{ max_part }}'
    AND _inserted_timestamp > '{{ max_is }}'
{% endif %}

qualify ROW_NUMBER() over (
    PARTITION BY history_operation_id,
    "order"
    ORDER BY
        batch_insert_ts DESC,
        _inserted_timestamp DESC
) = 1
)
SELECT
    partition_id,
    partition_gte_id,
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
