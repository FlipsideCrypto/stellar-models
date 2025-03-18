{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"streamline_ledgers",
        "sql_limit" :"10000",
        "producer_batch_size" :"10000",
        "worker_batch_size" :"5000",
        "sql_source" :"{{this.identifier}}",
        "order_by_column": "sequence" }
    )
) }}

WITH ledgers AS (

    SELECT
        SEQUENCE
    FROM
        {{ ref("streamline__legders") }}
    EXCEPT
    SELECT
        SEQUENCE
    FROM
        {{ ref("streamline__ledgers_complete") }}
)
SELECT
    SEQUENCE,
    ROUND(
        SEQUENCE,
        -4
    ) :: INT AS partition_key,
    {{ target.database }}.live.udf_api(
        'GET',
        '{Service}/{Authentication}/ledgers/' || SEQUENCE,
        OBJECT_CONSTRUCT(),
        OBJECT_CONSTRUCT(),
        'Vault/prod/stellar/quicknode/mainnet'
    ) AS request
FROM
    ledgers
