-- depends_on: {{ ref('bronze__transactions') }}
{{ config(
    materialized = 'incremental',
    unique_key = "id",
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
        id :: INTEGER AS id,
        VALUE :transaction_hash :: STRING AS transaction_hash,
        VALUE :ledger_sequence :: INTEGER AS ledger_sequence,
        VALUE :account :: STRING AS account,
        VALUE :account_sequence :: INTEGER AS account_sequence,
        VALUE :max_fee :: INTEGER AS max_fee,
        VALUE :operation_count :: INTEGER AS operation_count,
        TO_TIMESTAMP(
            VALUE :created_at :: INT,
            6
        ) AS created_at,
        VALUE :memo_type :: STRING AS memo_type,
        VALUE :memo :: STRING AS memo,
        VALUE :time_bounds :: STRING AS time_bounds,
        VALUE :"successful" :: BOOLEAN AS SUCCESSFUL,
        VALUE :fee_charged :: INTEGER AS fee_charged,
        VALUE :inner_transaction_hash :: STRING AS inner_transaction_hash,
        VALUE :fee_account :: STRING AS fee_account,
        VALUE :new_max_fee :: INTEGER AS new_max_fee,
        VALUE :account_muxed :: STRING AS account_muxed,
        VALUE :fee_account_muxed :: STRING AS fee_account_muxed,
        VALUE :batch_id :: STRING AS batch_id,
        TO_TIMESTAMP(
            VALUE :batch_run_date :: INT,
            6
        ) AS batch_run_date,
        TO_TIMESTAMP(
            VALUE :batch_insert_ts :: INT,
            6
        ) AS batch_insert_ts,
        VALUE :ledger_bounds :: STRING AS ledger_bounds,
        VALUE :min_account_sequence :: INTEGER AS min_account_sequence,
        VALUE :min_account_sequence_age :: INTEGER AS min_account_sequence_age,
        VALUE :min_account_sequence_ledger_gap :: INTEGER AS min_account_sequence_ledger_gap,
        VALUE :tx_envelope :: STRING AS tx_envelope,
        VALUE :tx_result :: STRING AS tx_result,
        VALUE :tx_meta :: STRING AS tx_meta,
        VALUE :tx_fee_meta :: STRING AS tx_fee_meta,
        VALUE :extra_signers :: ARRAY AS extra_signers,
        VALUE :resource_fee :: INTEGER AS resource_fee,
        VALUE :soroban_resources_instructions :: INTEGER AS soroban_resources_instructions,
        VALUE :soroban_resources_read_bytes :: INTEGER AS soroban_resources_read_bytes,
        VALUE :soroban_resources_write_bytes :: INTEGER AS soroban_resources_write_bytes,
        TO_TIMESTAMP(
            VALUE :closed_at :: INT,
            6
        ) AS closed_at,
        VALUE :transaction_result_code :: STRING AS transaction_result_code,
        VALUE :inclusion_fee_bid :: INTEGER AS inclusion_fee_bid,
        VALUE :inclusion_fee_charged :: INTEGER AS inclusion_fee_charged,
        VALUE :resource_fee_refund :: INTEGER AS resource_fee_refund,
        VALUE :non_refundable_resource_fee_charged :: INTEGER AS non_refundable_resource_fee_charged,
        VALUE :refundable_resource_fee_charged :: INTEGER AS refundable_resource_fee_charged,
        VALUE :rent_fee_charged :: INTEGER AS rent_fee_charged,
        VALUE :tx_signers :: STRING AS tx_signers,
        VALUE :refundable_fee :: INTEGER AS refundable_fee,
        _inserted_timestamp
    FROM

{% if is_incremental() %}
{{ ref('bronze__transactions') }}
{% else %}
    {{ ref('bronze__transactions_FR') }}
{% endif %}

{% if is_incremental() %}
WHERE
    partition_gte_id >= '{{ max_part }}'
    AND _inserted_timestamp > '{{ max_is }}'
{% endif %}

qualify ROW_NUMBER() over (
    PARTITION BY id
    ORDER BY
        batch_insert_ts DESC,
        _inserted_timestamp DESC
) = 1
)
SELECT
    partition_id,
    partition_gte_id,
    id,
    transaction_hash,
    ledger_sequence,
    account,
    account_sequence,
    max_fee,
    operation_count,
    created_at,
    memo_type,
    memo,
    time_bounds,
    SUCCESSFUL,
    fee_charged,
    inner_transaction_hash,
    fee_account,
    new_max_fee,
    account_muxed,
    fee_account_muxed,
    batch_id,
    batch_run_date,
    batch_insert_ts,
    ledger_bounds,
    min_account_sequence,
    min_account_sequence_age,
    min_account_sequence_ledger_gap,
    tx_envelope,
    tx_result,
    tx_meta,
    tx_fee_meta,
    extra_signers,
    resource_fee,
    soroban_resources_instructions,
    soroban_resources_read_bytes,
    soroban_resources_write_bytes,
    closed_at,
    transaction_result_code,
    inclusion_fee_bid,
    inclusion_fee_charged,
    resource_fee_refund,
    non_refundable_resource_fee_charged,
    refundable_resource_fee_charged,
    rent_fee_charged,
    tx_signers,
    refundable_fee,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['id']
    ) }} AS transactions_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    pre_final
