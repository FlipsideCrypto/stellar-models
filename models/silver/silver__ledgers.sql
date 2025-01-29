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
        SEQUENCE :: INTEGER AS SEQUENCE,
        ledger_hash :: STRING AS ledger_hash,
        previous_ledger_hash :: STRING AS previous_ledger_hash,
        transaction_count :: INTEGER AS transaction_count,
        operation_count :: INTEGER AS operation_count,
        closed_at :: TIMESTAMP AS closed_at,
        id :: INTEGER AS id,
        total_coins :: INTEGER AS total_coins,
        fee_pool :: INTEGER AS fee_pool,
        base_fee :: INTEGER AS base_fee,
        base_reserve :: INTEGER AS base_reserve,
        max_tx_set_size :: INTEGER AS max_tx_set_size,
        protocol_version :: INTEGER AS protocol_version,
        ledger_header :: BINARY AS ledger_header,
        successful_transaction_count :: INTEGER AS successful_transaction_count,
        failed_transaction_count :: INTEGER AS failed_transaction_count,
        tx_set_operation_count :: INTEGER AS tx_set_operation_count,
        batch_id :: STRING AS batch_id,
        batch_run_date :: TIMESTAMP AS batch_run_date,
        batch_insert_ts :: TIMESTAMP AS batch_insert_ts,
        soroban_fee_write_1kb :: INTEGER AS soroban_fee_write_1kb,
        node_id :: STRING AS node_id,
        signature :: STRING AS signature,
        total_byte_size_of_bucket_list :: INTEGER AS total_byte_size_of_bucket_list,
        _inserted_timestamp
    FROM

{% if is_incremental() %}
{{ ref('bronze__ledgers') }}
{% else %}
    {{ ref('bronze__ledgers_FR') }}
{% endif %}

{% if is_incremental() %}
WHERE
    partition_id >= '{{ max_part }}'
    AND _inserted_timestamp > '{{ max_inserted_timestamp }}'
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
