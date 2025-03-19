{{ config (
    materialized = "view",
    tags = ['streamline_view']
) }}

SELECT
    _id AS SEQUENCE
FROM
    {{ source(
        'crosschain_silver',
        'number_sequence'
    ) }}
WHERE
    _id >= (
        SELECT
            MIN(tail_sequence)
        FROM
            {{ ref('streamline__chain_head_tail') }}
    )
    AND _id <= (
        SELECT
            MAX(head_sequence)
        FROM
            {{ ref('streamline__chain_head_tail') }}
    )
