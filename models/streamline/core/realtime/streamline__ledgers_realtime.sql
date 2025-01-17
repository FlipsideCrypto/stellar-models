{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"ledgers",
        "sql_limit" :"500",
        "producer_batch_size" :"500",
        "worker_batch_size" :"500",
        "sql_source" :"{{this.identifier}}",
        "order_by_column": "ledger_sequence" }
    )
) }}

WITH ledgers AS (

    SELECT
        ledger_sequence
    FROM
        {{ ref("streamline__legders") }}
        {# EXCEPT
    SELECT
        block_number
    FROM
        {{ ref("streamline__blocks_complete") }}
        #}
)
SELECT
    ledger_sequence,
    ROUND(
        ledger_sequence,
        -4
    ) :: INT AS partition_key,
    {{ target.database }}.live.udf_api(
        'GET',
        '{Service}/{Authentication}/ledgers/' || ledger_sequence,
        OBJECT_CONSTRUCT(),
        OBJECT_CONSTRUCT(),
        'Vault/prod/stellar/quicknode/mainnet'
    ) AS request
FROM
    ledgers
