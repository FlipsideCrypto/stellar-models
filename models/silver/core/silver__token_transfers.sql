-- depends_on: {{ ref('bronze__token_transfers') }}
{{ config(
    materialized = 'incremental',
    unique_key = "token_transfers_id",
    incremental_predicates = ["dynamic_range_predicate", "partition_id::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['closed_at::DATE','partition_id','modified_timestamp::DATE'],
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
        VALUE :amount :: FLOAT AS amount,
        VALUE :amount_raw :: STRING AS amount_raw,
        VALUE :asset :: STRING AS asset,
        VALUE :asset_code :: STRING AS asset_code,
        VALUE :asset_issuer :: STRING AS asset_issuer,
        VALUE :asset_type :: STRING AS asset_type,
        TO_TIMESTAMP(
            VALUE :closed_at :: INT,
            6
        ) AS closed_at,
        VALUE :contract_id :: STRING AS contract_id,
        VALUE :event_topic :: STRING AS event_topic,
        VALUE :from :: STRING AS from_address,
        VALUE :ledger_sequence :: bigint AS ledger_sequence,
        VALUE :operation_id :: bigint AS operation_id,
        VALUE :to :: STRING AS to_address,
        VALUE :to_muxed :: STRING AS to_muxed,
        VALUE :to_muxed_id :: STRING AS to_muxed_id,
        VALUE :transaction_hash :: STRING AS transaction_hash,
        VALUE :transaction_id :: bigint AS transaction_id,
        VALUE :batch_id :: STRING AS batch_id,
        TO_TIMESTAMP(
            VALUE :batch_run_date :: INT,
            6
        ) AS batch_run_date,
        TO_TIMESTAMP(
            VALUE :batch_insert_ts :: INT,
            6
        ) AS batch_insert_ts,
        _inserted_timestamp,
        ROW_NUMBER() over(
            PARTITION BY transaction_hash,
            COALESCE(
                operation_id,
                0
            ),
            to_address,
            from_address,
            asset,
            amount_raw,
            event_topic,
            file_name
            ORDER BY
                _inserted_timestamp DESC
        ) AS artificial_uk
    FROM

{% if is_incremental() %}
{{ ref('bronze__token_transfers') }}
{% else %}
    {{ ref('bronze__token_transfers_FR') }}
{% endif %}

{% if is_incremental() %}
WHERE
    partition_gte_id >= '{{ max_part }}'
    AND _inserted_timestamp > '{{ max_is }}'
{% endif %}

qualify DENSE_RANK() over(
    PARTITION BY transaction_hash,
    COALESCE(
        operation_id,
        0
    ),
    to_address,
    from_address,
    asset,
    amount_raw,
    event_topic
    ORDER BY
        _inserted_timestamp DESC
) = 1
)
SELECT
    partition_id,
    partition_gte_id,
    amount,
    amount_raw,
    asset,
    asset_code,
    asset_issuer,
    asset_type,
    closed_at,
    contract_id,
    event_topic,
    from_address,
    ledger_sequence,
    operation_id,
    to_address,
    to_muxed,
    to_muxed_id,
    transaction_hash,
    transaction_id,
    batch_id,
    batch_run_date,
    batch_insert_ts,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['transaction_hash', 'operation_id', 'to_address', 'from_address', 'asset', 'amount_raw','event_topic','artificial_uk']
    ) }} AS token_transfers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    pre_final
