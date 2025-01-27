-- depends_on: {{ ref('bronze__ledgers') }}
{{ config(
    materialized = 'incremental',
    unique_key = ["sequence"],
    incremental_predicates = ["dynamic_range_predicate", "partition_id::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['closed_at::DATE','partition_id','modified_timestamp::DATE'],
    full_refresh = false,
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
        SEQUENCE,
        ledger_hash,
        previous_ledger_hash,
        transaction_count,
        operation_count,
        closed_at,
        id,
        total_coins,
        fee_pool,
        base_fee,
        base_reserve,
        max_tx_set_size,
        protocol_version,
        ledger_header,
        successful_transaction_count,
        failed_transaction_count,
        tx_set_operation_count,
        batch_id,
        batch_run_date,
        batch_insert_ts,
        soroban_fee_write_1kb,
        node_id,
        signature,
        total_byte_size_of_bucket_list,
        _inserted_timestamp
    FROM

{% if is_incremental() %}
{{ ref('bronze__ledgers') }}
{% else %}
    {{ ref('bronze__ledgers_FR') }}
{% endif %}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= '{{ max_inserted_timestamp }}'
{% endif %}

qualify ROW_NUMBER() over (
    PARTITION BY SEQUENCE
    ORDER BY
        _inserted_timestamp DESC
) = 1
)
SELECT
    partition_id,
    SEQUENCE,
    ledger_hash,
    previous_ledger_hash,
    transaction_count,
    operation_count,
    closed_at,
    id,
    total_coins,
    fee_pool,
    base_fee,
    base_reserve,
    max_tx_set_size,
    protocol_version,
    ledger_header,
    successful_transaction_count,
    failed_transaction_count,
    tx_set_operation_count,
    batch_id,
    batch_run_date,
    batch_insert_ts,
    soroban_fee_write_1kb,
    node_id,
    signature,
    total_byte_size_of_bucket_list,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['sequence']
    ) }} AS ledgers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    pre_final
