{{ config (
    materialized = "view",
    tags = ['streamline_view']
) }}

WITH res AS (

    SELECT
        {{ target.database }}.live.udf_api(
            'POST',
            '{Service}/{Authentication}',
            OBJECT_CONSTRUCT(
                'Content-Type',
                'application/json',
                'fsc-quantum-state',
                'livequery'
            ),
            OBJECT_CONSTRUCT(
                'id',
                0,
                'jsonrpc',
                '2.0',
                'method',
                'getLatestLedger'
            ),
            'Vault/prod/stellar/quicknode/mainnet'
        ) :data :result AS DATA
)
SELECT
    DATA :id :: STRING AS ledger_id,
    DATA :protocolVersion :: INT AS protocol_version,
    DATA :sequence :: INT AS SEQUENCE
FROM
    res
