{{ config (
    materialized = "view",
    tags = ['streamline_view']
) }}

WITH head AS (

    SELECT
        {{ target.database }}.live.udf_api(
            'GET',
            '{Service}/{Authentication}/ledgers?limit=1&order=desc',
            OBJECT_CONSTRUCT(),
            OBJECT_CONSTRUCT(),
            'Vault/prod/stellar/quicknode/mainnet'
        ) :data :_embedded :records [0] AS DATA,
        DATA :sequence :: INT AS ledger_sequence,
        DATA :closed_at :: datetime AS block_timestamp
),
tail AS (
    SELECT
        {{ target.database }}.live.udf_api(
            'GET',
            '{Service}/{Authentication}/ledgers?limit=1&order=asc',
            OBJECT_CONSTRUCT(),
            OBJECT_CONSTRUCT(),
            'Vault/prod/stellar/quicknode/mainnet'
        ) :data :_embedded :records [0] AS DATA,
        DATA :sequence :: INT AS ledger_sequence,
        DATA :closed_at :: datetime AS block_timestamp
)
SELECT
    A.ledger_sequence AS head_ledger_sequence,
    A.block_timestamp AS head_block_timestamp,
    b.ledger_sequence AS tail_ledger_sequence,
    b.block_timestamp AS tail_block_timestamp
FROM
    head A
    JOIN tail b
