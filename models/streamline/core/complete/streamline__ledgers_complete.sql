{{ config (
    materialized = "incremental",
    incremental_strategy = 'merge',
    unique_key = "sequence",
    cluster_by = "block_timestamp::DATE",
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION on equality(sequence)"
) }}
-- depends_on: {{ ref('bronze__streamline_ledgers') }}

SELECT
    DATA :sequence :: INT AS SEQUENCE,
    TO_TIMESTAMP(
        DATA :closed_at
    ) AS block_timestamp,
    DATA :successful_transaction_count :: INT AS successful_transaction_count,
    DATA :failed_transaction_count :: INT AS failed_transaction_count,
    DATA :operation_count :: INT AS operation_count,
    DATA :tx_set_operation_count :: INT AS tx_set_operation_count,
    {{ dbt_utils.generate_surrogate_key(
        ['sequence']
    ) }} AS complete_ledgers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM

{% if is_incremental() %}
{{ ref('bronze__streamline_ledgers') }}
WHERE
    inserted_timestamp >= (
        SELECT
            MAX(modified_timestamp) modified_timestamp
        FROM
            {{ this }}
    )
{% else %}
    {{ ref('bronze__streamline_ledgers_FR') }}
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY SEQUENCE
ORDER BY
    inserted_timestamp DESC)) = 1
