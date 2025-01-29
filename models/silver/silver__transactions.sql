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
        id :: INTEGER AS id,
        transaction_hash :: STRING AS transaction_hash,
        ledger_sequence :: INTEGER AS ledger_sequence,
        account :: STRING AS account,
        account_sequence :: INTEGER AS account_sequence,
        max_fee :: INTEGER AS max_fee,
        operation_count :: INTEGER AS operation_count,
        created_at :: TIMESTAMP AS created_at,
        memo_type :: STRING AS memo_type,
        memo :: STRING AS memo,
        time_bounds :: STRING AS time_bounds,
        SUCCESSFUL :: BOOLEAN AS SUCCESSFUL,
        fee_charged :: INTEGER AS fee_charged,
        inner_transaction_hash :: STRING AS inner_transaction_hash,
        fee_account :: STRING AS fee_account,
        new_max_fee :: INTEGER AS new_max_fee,
        account_muxed :: STRING AS account_muxed,
        fee_account_muxed :: STRING AS fee_account_muxed,
        batch_id :: STRING AS batch_id,
        batch_run_date :: TIMESTAMP AS batch_run_date,
        batch_insert_ts :: TIMESTAMP AS batch_insert_ts,
        ledger_bounds :: STRING AS ledger_bounds,
        min_account_sequence :: INTEGER AS min_account_sequence,
        min_account_sequence_age :: INTEGER AS min_account_sequence_age,
        min_account_sequence_ledger_gap :: INTEGER AS min_account_sequence_ledger_gap,
        tx_envelope :: STRING AS tx_envelope,
        tx_result :: STRING AS tx_result,
        tx_meta :: STRING AS tx_meta,
        tx_fee_meta :: STRING AS tx_fee_meta,
        extra_signers :: ARRAY AS extra_signers,
        resource_fee :: INTEGER AS resource_fee,
        soroban_resources_instructions :: INTEGER AS soroban_resources_instructions,
        soroban_resources_read_bytes :: INTEGER AS soroban_resources_read_bytes,
        soroban_resources_write_bytes :: INTEGER AS soroban_resources_write_bytes,
        closed_at :: TIMESTAMP AS closed_at,
        transaction_result_code :: STRING AS transaction_result_code,
        inclusion_fee_bid :: INTEGER AS inclusion_fee_bid,
        inclusion_fee_charged :: INTEGER AS inclusion_fee_charged,
        resource_fee_refund :: INTEGER AS resource_fee_refund,
        non_refundable_resource_fee_charged :: INTEGER AS non_refundable_resource_fee_charged,
        refundable_resource_fee_charged :: INTEGER AS refundable_resource_fee_charged,
        rent_fee_charged :: INTEGER AS rent_fee_charged,
        tx_signers :: STRING AS tx_signers,
        refundable_fee :: INTEGER AS refundable_fee,
        _inserted_timestamp
    FROM

{% if is_incremental() %}
{{ ref('bronze__transactions') }}
{% else %}
    {{ ref('bronze__transactions_FR') }}
{% endif %}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= '{{ max_inserted_timestamp }}'
{% endif %}

qualify ROW_NUMBER() over (
    PARTITION BY id
    ORDER BY
        _inserted_timestamp DESC
) = 1
)
SELECT
    partition_id,
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
