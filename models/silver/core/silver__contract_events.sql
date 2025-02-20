-- depends_on: {{ ref('bronze__contract_events') }}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ["transaction_hash","closed_at::DATE"],
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
        VALUE :transaction_hash :: STRING AS transaction_hash,
        VALUE :transaction_id :: INTEGER AS transaction_id,
        TO_TIMESTAMP(
            VALUE :closed_at :: INT,
            6
        ) AS closed_at,
        VALUE :ledger_sequence :: INTEGER AS ledger_sequence,
        VALUE :"successful" :: BOOLEAN AS SUCCESSFUL,
        VALUE :in_successful_contract_call :: BOOLEAN AS in_successful_contract_call,
        VALUE :contract_id :: STRING AS contract_id,
        VALUE :type :: INTEGER AS TYPE,
        VALUE :type_string :: STRING AS type_string,
        TRY_PARSE_JSON(
            VALUE :topics
        ) AS topics,
        TRY_PARSE_JSON(
            VALUE :topics_decoded
        ) AS topics_decoded,
        TRY_PARSE_JSON(
            VALUE :data
        ) AS DATA,
        TRY_PARSE_JSON(
            VALUE :data_decoded
        ) AS data_decoded,
        VALUE :contract_event_xdr :: STRING AS contract_event_xdr,
        VALUE :batch_id :: STRING AS batch_id,
        TO_TIMESTAMP(
            VALUE :batch_run_date :: INT,
            6
        ) AS batch_run_date,
        TO_TIMESTAMP(
            VALUE :batch_insert_ts :: INT,
            6
        ) AS batch_insert_ts,
        _inserted_timestamp
    FROM

{% if is_incremental() %}
{{ ref('bronze__contract_events') }}
{% else %}
    {{ ref('bronze__contract_events_FR') }}
{% endif %}

{% if is_incremental() %}
WHERE
    partition_gte_id >= '{{ max_part }}'
    AND _inserted_timestamp > '{{ max_is }}'
{% endif %}

{# this is intentionally a rank and not a row_num due to no true PK on the table #}
qualify RANK() over (
    PARTITION BY transaction_hash
    ORDER BY
        batch_insert_ts DESC,
        _inserted_timestamp DESC
) = 1
)
SELECT
    partition_id,
    partition_gte_id,
    transaction_hash,
    transaction_id,
    closed_at,
    ledger_sequence,
    SUCCESSFUL,
    in_successful_contract_call,
    contract_id,
    TYPE,
    type_string,
    topics,
    topics_decoded,
    DATA,
    data_decoded,
    contract_event_xdr,
    batch_id,
    batch_run_date,
    batch_insert_ts,
    -- this isn't the order they were executed in but just a random rank to get a PK
    ROW_NUMBER() over (
        PARTITION BY transaction_hash
        ORDER BY
            SYSDATE()
    ) AS _event_order,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['transaction_hash','_event_order']
    ) }} AS contract_events_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    pre_final
