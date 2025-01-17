{{ config (
    materialized = "view",
    tags = ['streamline_view']
) }}

SELECT
    _id AS ledger_sequence
FROM
    {{ source(
        'crosschain_silver',
        'number_sequence'
    ) }}
WHERE
    _id >= (
        SELECT
            MIN(tail_ledger_sequence)
        FROM
            {{ ref('streamline__chain_head_tail') }}
    )
    AND _id <= (
        SELECT
            MAX(head_ledger_sequence)
        FROM
            {{ ref('streamline__chain_head_tail') }}
    )
