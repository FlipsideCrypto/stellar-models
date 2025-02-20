-- depends_on: {{ ref('silver__contract_events') }}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ["transaction_hash","closed_at::DATE"],
    cluster_by = ['block_timestamp::DATE','closed_at::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(transaction_id,transaction_hash,contract_id,topics_decoded,data_decoded);",
    tags = ['scheduled_core']
) }}

SELECT
    transaction_hash,
    transaction_id,
    closed_at,
    closed_at AS block_timestamp,
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
    {{ dbt_utils.generate_surrogate_key(
        ['transaction_hash','_event_order']
    ) }} AS fact_contract_events_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ ref('silver__contract_events') }}

{% if is_incremental() %}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
